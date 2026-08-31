#!/usr/bin/env python3
"""Apply bounded date-overlap updates for tables without ``lastupdated``."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.auth import client_from_vault
from sharadar_pipeline.catalog import SharadarTable
from sharadar_pipeline.routes import PRODUCTION_BACKFILL_CONFIRMATION
from sharadar_pipeline.runtime import connect_mongo_runtime
from sharadar_pipeline.schema_registry import load_schema_registry
from sharadar_pipeline.storage import ArtifactStore, MongoCurrentStore, VendorStorageEngine

DATE_OVERLAP_TABLES = (SharadarTable.ACTIONS, SharadarTable.EVENTS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update actions/events through a bounded date overlap."
    )
    parser.add_argument("--deployment", choices=("dev", "prod"), required=True)
    parser.add_argument(
        "--table",
        action="append",
        choices=[table.value for table in DATE_OVERLAP_TABLES],
    )
    parser.add_argument(
        "--as-of", type=date.fromisoformat, default=datetime.now(UTC).date()
    )
    parser.add_argument("--lookback-days", type=int, default=35)
    parser.add_argument("--actions-forward-days", type=int, default=370)
    parser.add_argument("--confirmation", required=True)
    parser.add_argument("--production-confirmation")
    parser.add_argument("--row-batch-size", type=int, default=10_000)
    return parser.parse_args()


def date_window(
    table: SharadarTable,
    as_of: date,
    *,
    lookback_days: int,
    actions_forward_days: int,
) -> tuple[date, date]:
    start = as_of - timedelta(days=lookback_days)
    end = (
        as_of + timedelta(days=actions_forward_days)
        if table is SharadarTable.ACTIONS
        else as_of
    )
    return start, end


def _write_delta_jsonl(
    *,
    client: Any,
    table: SharadarTable,
    start: date,
    end: date,
    service_date: date,
    staging: Path,
) -> tuple[Path, int]:
    staging.mkdir(parents=True, exist_ok=True)
    count = 0
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix=f"{table.value}-date-overlap-",
        suffix=".jsonl",
        dir=staging,
        delete=False,
    ) as handle:
        path = Path(handle.name)
        handle.write(
            json.dumps(
                {
                    "capture_format": "sharadar.date-overlap/v1",
                    "table": table.value,
                    "service_date": service_date.isoformat(),
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                },
                sort_keys=True,
            )
            + "\n"
        )
        for row in client.iter_json_rows(
            table,
            params={
                "from": start.isoformat(),
                "to": end.isoformat(),
                "sort": "date.asc",
            },
            page_size=10_000,
            max_pages=100,
        ):
            handle.write(
                json.dumps(row, sort_keys=True, ensure_ascii=False, default=str) + "\n"
            )
            count += 1
    return path, count


def _read_rows(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        next(handle)
        for line in handle:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise TypeError("date-overlap JSONL row must be an object")
            yield value


def main() -> int:
    args = parse_args()
    if args.lookback_days < 1:
        raise SystemExit("--lookback-days must be positive")
    if args.actions_forward_days < 0:
        raise SystemExit("--actions-forward-days cannot be negative")
    if args.deployment == "prod" and (
        args.production_confirmation != PRODUCTION_BACKFILL_CONFIRMATION
    ):
        raise SystemExit(
            "PROD requires --production-confirmation BACKFILL_SHARADAR_PROD"
        )

    runtime = connect_mongo_runtime(
        args.deployment,
        write=True,
        confirmation=args.confirmation,
        production_confirmation=args.production_confirmation,
    )
    try:
        root = runtime.route.artifact_root
        registry = load_schema_registry()
        artifacts = ArtifactStore(root)
        engine = VendorStorageEngine(
            artifacts,
            MongoCurrentStore(runtime.database, batch_size=args.row_batch_size),
            row_batch_size=args.row_batch_size,
        )
        client = client_from_vault()
        tables = tuple(
            SharadarTable(value)
            for value in (args.table or [table.value for table in DATE_OVERLAP_TABLES])
        )
        for table in tables:
            start, end = date_window(
                table,
                args.as_of,
                lookback_days=args.lookback_days,
                actions_forward_days=args.actions_forward_days,
            )
            path, row_count = _write_delta_jsonl(
                client=client,
                table=table,
                start=start,
                end=end,
                service_date=args.as_of,
                staging=root / "staging",
            )
            try:
                if row_count == 0:
                    print(
                        json.dumps(
                            {
                                "status": "NO_CHANGES",
                                "table": table.value,
                                "from": start.isoformat(),
                                "to": end.isoformat(),
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                    continue
                raw = artifacts.capture_file(table.value, path, suffix=".jsonl")
                receipt = engine.ingest_rows(
                    table.value,
                    raw,
                    _read_rows(path),
                    registry,
                    source_watermark=args.as_of.isoformat(),
                    pipeline_version="date-overlap-v1",
                )
                print(
                    json.dumps(
                        {
                            "status": "TABLE_COMPLETE",
                            "table": table.value,
                            "rows": receipt.parquet.row_count,
                            "from": start.isoformat(),
                            "to": end.isoformat(),
                            "service_date": args.as_of.isoformat(),
                            "run_id": receipt.run_id,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
            finally:
                path.unlink(missing_ok=True)
    finally:
        runtime.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
