#!/usr/bin/env python3
"""Live, read-only entitlement canary for the Fundamentals subscription."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from pathlib import Path


MEDINA_ROOT = Path(__file__).resolve().parents[2]
if str(MEDINA_ROOT) not in sys.path:
    sys.path.insert(0, str(MEDINA_ROOT))

from sharadar_pipeline import SharadarTable, client_from_vault
from sharadar_pipeline.catalog import TABLE_SPECS, Plan


PLAN_TABLES = tuple(
    table
    for table, spec in TABLE_SPECS.items()
    if Plan.FUNDAMENTALS in spec.plans
)
TICKER_TABLES = frozenset(
    {SharadarTable.TICKERS, SharadarTable.FUNDAMENTALS, SharadarTable.DAILY}
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check paid Sharadar Fundamentals endpoints without printing data."
    )
    parser.add_argument("--ticker", default="PLTR")
    parser.add_argument("--secret-name", default="sharadar_api_key")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--skip-bulk-redirects",
        action="store_true",
        help="Check status metadata but do not request signed bulk locations.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = client_from_vault(
        secret_name=args.secret_name,
        timeout=(10.0, args.timeout),
    )

    # Public schemas exercise every catalog route, including tables outside the
    # paid plan. These requests carry no API key.
    for table in SharadarTable:
        schema = client.table_schema(table, dialect="postgres")
        if not schema.strip():
            raise RuntimeError(f"empty public schema for {table.value}")
    print(f"schema_check=OK tables={len(SharadarTable)}")

    # Authenticated checks are limited to the active Fundamentals plan.
    for table in PLAN_TABLES:
        params: dict[str, object] = {"limit": 1}
        if table in TICKER_TABLES:
            params["ticker"] = args.ticker
        payload = client.query_json(table, params=params)
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), list):
            raise RuntimeError(f"unexpected JSON envelope for {table.value}")
        rows = payload["data"]
        ticker_matches = None
        if table in TICKER_TABLES:
            ticker_matches = bool(rows) and rows[0].get("ticker") == args.ticker
            if not ticker_matches:
                raise RuntimeError(f"ticker mismatch for {table.value}")
        print(
            f"data_table={table.value} status=OK rows={len(rows)} "
            f"ticker_matches={ticker_matches}"
        )

        status = client.bulk_status(table)
        if not status:
            raise RuntimeError(f"empty bulk status for {table.value}")
        print(f"bulk_status={table.value} status=OK")

        if not args.skip_bulk_redirects:
            redirect = client.bulk_redirect(table, "full")
            # Never print the location, its host, query or repr.
            print(
                f"bulk_redirect={redirect.table} history={redirect.history} status=302"
            )

    print(
        f"sharadar_access_check=OK plan={Plan.FUNDAMENTALS.value} "
        f"entitled_tables={len(PLAN_TABLES)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
