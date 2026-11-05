#!/usr/bin/env python3
"""Incremental ETL fetch of JSON API rows into timestamped CSV files.
The API cannot filter on ``created_at``/``updated_at`` as a generic field, but
it does support ``updatedAt_gt`` / ``updatedAt_gte`` against the real column.
A new row always has ``updated_at == created_at``, so a single "modified since"
cursor picks up inserts *and* edits.
Usage:
    python3 etl/main.py --modified-since 2026-11-04T09:12:33Z --token jsonsrv_...
Each fetched page is written to its own CSV file (with a header row) in the
output folder, named after the run start time plus the page number:
    etl/output/20261104T091500Z_p001.csv
At the end the script prints the value to use as ``--modified-since`` next time:
    NEXT_MODIFIED_SINCE=2026-11-04T10:03:11.482913Z
Store that value in your warehouse/state table *in the same transaction* that
loads the CSVs, so a failed load never advances the cursor.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

# Cloudflare in front of aspcode.net rejects unknown client signatures with
# error 1010, so the default has to look like an ordinary browser.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

DEFAULT_URL = "https://aspcode.net/api/db/Orders/orders"
DEFAULT_LIMIT = 500  # the API ignores anything above 500 and falls back to 50
MAX_PAGES = 1000  # runaway guard for a single run
MAX_RETRIES = 3
TIMEOUT_SECONDS = 60

ENVELOPE_COLUMNS = ("id", "database", "resource", "createdAt", "updatedAt")


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incrementally fetch JSON API rows and store them as CSV files.",
    )
    parser.add_argument(
        "--modified-since",
        required=True,
        help="Watermark: fetch rows with updated_at >= this time "
        "(RFC3339 such as 2026-11-04T09:12:33Z, or 2026-11-04).",
    )
    parser.add_argument(
        "--token",
        help="API key sent as X-API-Key. Falls back to the ASPCODE_TOKEN environment variable.",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"Resource endpoint to read (default: {DEFAULT_URL}).",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header; change it if Cloudflare blocks the request with error 1010.",
    )
    parser.add_argument(
        "--watermark-file",
        help="Write the next modified_since value to this file as plain text, so a "
        "scheduler (for example SSIS) can read it without parsing stdout.",
    )
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parent / "output"),
        help="Folder for the CSV files (default: ./output next to this script).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Rows per request, 1-500 (default: {DEFAULT_LIMIT}).",
    )

    args = parser.parse_args(argv)

    if args.token is None or args.token == "":
        args.token = os.environ.get("ASPCODE_TOKEN", "")
    if not args.token:
        parser.error("--token is required (or set ASPCODE_TOKEN)")

    if not 1 <= args.limit <= 500:
        parser.error("--limit must be between 1 and 500")

    return args


def datetime_from_wire(raw: str) -> datetime:
    """Parse a timestamp the API accepts, in UTC.
    The API emits RFC3339 with an optional fraction, so a watermark copied from
    a previous response round-trips. A bare date is read as UTC midnight, which
    is earlier than any row of that day and therefore safe for a watermark.
    """
    text = raw.strip()
    if text == "":
        raise ValueError("empty timestamp")

    if text[-1] in ("Z", "z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(
            f"invalid timestamp {raw!r}, expected RFC3339 such as 2026-11-04T09:12:33Z"
        ) from None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_page_url(base: str, modified_since: str, limit: int, offset: int) -> str:
    """Build a page request: ascending updated_at, so pages stay stable."""
    query = urllib.parse.urlencode(
        {
            "limit": limit,
            "offset": offset,
            "sort": "updated_at",
            "updatedAt_gte": modified_since,
        }
    )
    separator = "&" if urllib.parse.urlparse(base).query else "?"
    return f"{base}{separator}{query}"


def fetch_page(url: str, token: str, user_agent: str) -> list[dict[str, Any]]:
    """Fetch one page, retrying transient failures with a short backoff."""
    request = urllib.request.Request(
        url,
        headers={
            "X-API-Key": token,
            "Accept": "application/json",
            "User-Agent": user_agent,
        },
    )

    last_error = "unknown error"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            if not isinstance(payload, list):
                raise ValueError("expected a JSON array from the API")
            return payload
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")[:400]
            if exc.code in (400, 401, 403):
                # A bad watermark or key will not fix itself; fail loudly.
                hint = ""
                if exc.code == 403 and "1010" in body:
                    hint = (
                        "\nhint: Cloudflare error 1010 blocks the client's browser signature. "
                        "Retry with --user-agent 'curl/8.10.1' or another client string the "
                        "site allows."
                    )
                raise SystemExit(f"request failed ({exc.code}): {body}{hint}") from None
            last_error = f"HTTP {exc.code}: {body}"
        except urllib.error.URLError as exc:
            last_error = f"connection error: {exc.reason}"
        except ValueError as exc:
            raise SystemExit(f"unexpected response: {exc}") from None

        if attempt < MAX_RETRIES:
            time.sleep(2 ** (attempt - 1))

    raise SystemExit(f"giving up after {MAX_RETRIES} attempts: {last_error}")


def csv_cell(value: Any) -> str:
    """Render one JSON value as a CSV cell; nested values stay valid JSON."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def write_csv(rows: Sequence[dict[str, Any]], path: Path) -> list[str]:
    """Write one page to CSV with a header row and return the header."""
    payloads = [row.get("data") or {} for row in rows]

    # JSON objects do not have a fixed schema, so the header is the union of
    # every key seen on this page. A shape change therefore grows the header
    # for the pages written after it, never corrupts an earlier file.
    data_columns = sorted({key for payload in payloads for key in payload})
    header = list(ENVELOPE_COLUMNS) + data_columns

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row, payload in zip(rows, payloads):
            writer.writerow(
                [
                    csv_cell(row.get(column))
                    if column in ENVELOPE_COLUMNS
                    else csv_cell(payload.get(column))
                    for column in header
                ]
            )

    return header


def page_watermark(rows: Sequence[dict[str, Any]]) -> str | None:
    """Return the newest updatedAt on the page, as the API formatted it."""
    stamps = [
        (datetime_from_wire(row["updatedAt"]), row["updatedAt"])
        for row in rows
        if row.get("updatedAt")
    ]
    if not stamps:
        return None
    return max(stamps, key=lambda item: item[0])[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        datetime_from_wire(args.modified_since)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    modified_since = args.modified_since.strip()
    out_dir = Path(args.out).expanduser().resolve()
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    offset = 0
    pages = 0
    total_rows = 0
    watermark = modified_since

    while pages < MAX_PAGES:
        url = build_page_url(args.url, modified_since, args.limit, offset)
        rows = fetch_page(url, args.token, args.user_agent)

        if not rows:
            break

        pages += 1
        total_rows += len(rows)

        path = out_dir / f"{run_stamp}_p{pages:03d}.csv"
        header = write_csv(rows, path)

        page_mark = page_watermark(rows)
        if page_mark is not None:
            watermark = page_mark

        print(
            f"{path} rows={len(rows)} columns={len(header)} "
            f"last_updated_at={page_mark or '-'}",
            flush=True,
        )

        offset += len(rows)
        if len(rows) < args.limit:
            break

    if pages >= MAX_PAGES:
        print(
            f"warning: stopped after {MAX_PAGES} pages; run again with "
            f"--modified-since {watermark} to continue",
            file=sys.stderr,
        )

    if args.watermark_file:
        mark_path = Path(args.watermark_file).expanduser()
        mark_path.parent.mkdir(parents=True, exist_ok=True)
        # Write via a temporary file so a reader never sees half a timestamp.
        tmp_path = mark_path.with_name(mark_path.name + ".tmp")
        tmp_path.write_text(watermark + "\n", encoding="utf-8")
        os.replace(tmp_path, mark_path)
        print(f"watermark_file={mark_path}")

    print(f"rows={total_rows} pages={pages} folder={out_dir}")
    print(f"NEXT_MODIFIED_SINCE={watermark}")
    return 0


if __name__ == "__main__":
    sys.exit(main())