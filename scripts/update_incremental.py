#!/usr/bin/env python3
"""Apply bounded lastupdated deltas to an existing Sharadar deployment."""

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
from sharadar_pipeline.storage import (
    ArtifactStore,
    MongoCurrentStore,
    VendorStorageEngine,
)

INCREMENTAL_TABLES = (
    SharadarTable.TICKERS,
    SharadarTable.FUNDAMENTALS,
    SharadarTable.DAILY,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply Sharadar lastupdated deltas with a bounded overlap window."
    )
    parser.add_argument("--deployment", choices=("dev", "prod"), required=True)
    parser.add_argument(
        "--table",
        action="append",
        choices=[table.value for table in INCREMENTAL_TABLES],
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=datetime.now(UTC).date(),
    )
    parser.add_argument("--from-date", type=date.fromisoformat)
    parser.add_argument("--overlap-days", type=int, default=3)
    parser.add_argument("--confirmation", required=True)
    parser.add_argument("--production-confirmation")
    parser.add_argument("--row-batch-size", type=int, default=10_000)
    return parser.parse_args()


def _watermark_date(root: Path, table: SharadarTable) -> date | None:
    path = root / "watermarks" / f"{table.value}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        value = payload.get("value")
        return date.fromisoformat(value) if isinstance(value, str) else None
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _write_delta_jsonl(
    *,
    client: Any,
    table: SharadarTable,
    start: date,
    end: date,
    staging: Path,
) -> tuple[Path, int, str | None]:
    staging.mkdir(parents=True, exist_ok=True)
    maximum: str | None = None
    count = 0
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix=f"{table.value}-delta-",
        suffix=".jsonl",
        dir=staging,
        delete=False,
    ) as handle:
        path = Path(handle.name)
        handle.write(
            json.dumps(
                {
                    "capture_format": "sharadar.lastupdated-delta/v1",
                    "table": table.value,
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
                "lastupdated.gte": start.isoformat(),
                "lastupdated.lte": end.isoformat(),
                "sort": "lastupdated.asc",
            },
            page_size=10_000,
            max_pages=100,
        ):
            value = row.get("lastupdated")
            if value and (maximum is None or str(value) > maximum):
                maximum = str(value)
            handle.write(
                json.dumps(row, sort_keys=True, ensure_ascii=False, default=str) + "\n"
            )
            count += 1
    return path, count, maximum


def _read_delta_rows(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        next(handle)
        for line in handle:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise TypeError("delta JSONL row must be an object")
            yield value


def main() -> int:
    args = parse_args()
    if args.overlap_days < 1:
        raise SystemExit("--overlap-days must be positive")
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
            for value in (args.table or [table.value for table in INCREMENTAL_TABLES])
        )
        for table in tables:
            previous = args.from_date or _watermark_date(root, table)
            if previous is None:
                raise RuntimeError(
                    f"{table.value}: no ISO watermark; provide --from-date"
                )
            start = previous - timedelta(days=args.overlap_days)
            path, row_count, maximum = _write_delta_jsonl(
                client=client,
                table=table,
                start=start,
                end=args.as_of,
                staging=root / "staging",
            )
            try:
                if row_count == 0:
                    print(
                        json.dumps(
                            {"status": "NO_CHANGES", "table": table.value},
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                    continue
                raw = artifacts.capture_file(table.value, path, suffix=".jsonl")
                receipt = engine.ingest_rows(
                    table.value,
                    raw,
                    _read_delta_rows(path),
                    registry,
                    source_watermark=maximum or args.as_of.isoformat(),
                    pipeline_version="incremental-v1",
                )
                print(
                    json.dumps(
                        {
                            "status": "TABLE_COMPLETE",
                            "table": table.value,
                            "rows": receipt.parquet.row_count,
                            "watermark": maximum,
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
