#!/usr/bin/env python3
"""Verify Sharadar Fundamentals entitlement without exposing credentials/data."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


MEDINA_ROOT = Path(__file__).resolve().parents[2]
if str(MEDINA_ROOT) not in sys.path:
    sys.path.insert(0, str(MEDINA_ROOT))

from hub.vault import get_secret  # noqa: E402


API_BASE = "https://api.sharadar.com/v1.0/data"
USER_AGENT = "Medina-SharadarV1/0.1"
TABLES = ("fundamentals", "daily", "tickers")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _json_slice(
    table: str, ticker: str, key: str, timeout: float
) -> tuple[int, int, bool]:
    query = urllib.parse.urlencode(
        {"ticker": ticker, "limit": 1, "format": "json"}
    )
    request = urllib.request.Request(
        f"{API_BASE}/{table}?{query}",
        headers={
            "x-api-key": key,
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        payload = json.loads(body)
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        ticker_matches = bool(rows) and rows[0].get("ticker") == ticker
        return response.status, len(rows), ticker_matches


def _bulk_redirect_available(key: str, timeout: float) -> bool:
    query = urllib.parse.urlencode({"years": "full"})
    request = urllib.request.Request(
        f"{API_BASE}/fundamentals?{query}",
        headers={"x-api-key": key, "User-Agent": USER_AGENT},
    )
    try:
        urllib.request.build_opener(NoRedirect).open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        if exc.code != 302:
            raise
        location = exc.headers.get("Location", "")
        return bool(urllib.parse.urlparse(location).netloc)
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check paid Sharadar Fundamentals access with safe metadata only."
    )
    parser.add_argument("--ticker", default="PLTR")
    parser.add_argument("--secret-name", default="sharadar_api_key")
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    key = get_secret(args.secret_name)
    if not isinstance(key, str) or not key.strip():
        raise RuntimeError(f"Vault secret {args.secret_name!r} is empty or not text")

    try:
        for table in TABLES:
            status, rows, matches = _json_slice(
                table, args.ticker, key, args.timeout
            )
            if status != 200 or rows != 1 or not matches:
                raise RuntimeError(
                    f"unexpected {table} response: "
                    f"status={status} rows={rows} ticker_matches={matches}"
                )
            print(
                f"table={table} status={status} rows={rows} "
                f"requested_ticker_matches={matches}"
            )

        bulk_ok = _bulk_redirect_available(key, args.timeout)
        if not bulk_ok:
            raise RuntimeError("full-history fundamentals bulk redirect unavailable")
        print("bulk=fundamentals_full status=302 signed_download_host_present=True")
    except urllib.error.HTTPError as exc:
        # Do not print response bodies: vendors sometimes echo request metadata.
        raise RuntimeError(f"Sharadar request failed with HTTP {exc.code}") from None

    print("sharadar_access_check=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
