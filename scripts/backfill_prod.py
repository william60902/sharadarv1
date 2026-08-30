#!/usr/bin/env python3
"""Prepare or explicitly execute the SHARADAR_PROD full-history bulk backfill."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.auth import client_from_vault
from sharadar_pipeline.bulk_pipeline import ingest_bulk_table
from sharadar_pipeline.catalog import SharadarTable
from sharadar_pipeline.readiness import gate_specs_from_registry, verify_dev_readiness
from sharadar_pipeline.readiness_storage import DevStorageEvidenceSource
from sharadar_pipeline.routes import PRODUCTION_BACKFILL_CONFIRMATION
from sharadar_pipeline.runtime import connect_mongo_runtime
from sharadar_pipeline.schema_registry import FUNDAMENTALS_TABLES, load_schema_registry
from sharadar_pipeline.storage import (
    ArtifactStore,
    MongoCurrentStore,
    VendorStorageEngine,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify SHARADAR_DEV and prepare the full-history PROD bulk plan. "
            "No PROD I/O occurs unless --execute and both confirmations are supplied."
        )
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--table",
        action="append",
        choices=[table.value for table in FUNDAMENTALS_TABLES],
        help="Repeat to resume selected tables; omit for all seven.",
    )
    parser.add_argument("--confirmation")
    parser.add_argument("--production-confirmation")
    parser.add_argument("--row-batch-size", type=int, default=10_000)
    return parser.parse_args()


def _verify_live_dev(registry: object) -> dict[str, object]:
    runtime = connect_mongo_runtime("dev", write=False)
    try:
        report = verify_dev_readiness(
            DevStorageEvidenceSource(runtime.database, runtime.route.artifact_root),
            gate_specs_from_registry(registry),
            route=runtime.route,
        )
        if not report.ready_for_prod_backfill:
            raise RuntimeError(
                f"SHARADAR_DEV readiness has {report.failed_checks} failed checks"
            )
        return {
            "checked_at": report.checked_at,
            "failed_checks": report.failed_checks,
            "ready_for_prod_backfill": True,
            "prod_write_authorized": False,
        }
    finally:
        runtime.close()


def main() -> int:
    args = parse_args()
    registry = load_schema_registry()
    readiness = _verify_live_dev(registry)
    tables = tuple(
        SharadarTable(value) for value in (args.table or [t.value for t in FUNDAMENTALS_TABLES])
    )
    plan = {
        "database": "SHARADAR_PROD",
        "artifact_root": (
            "/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/prod"
        ),
        "history": "full",
        "mode": "bulk",
        "schema_registry_sha256": registry.resource_sha256,
        "tables": [table.value for table in tables],
        "dev_readiness": readiness,
        "execute": args.execute,
    }
    if not args.execute:
        print(json.dumps({"status": "READY", "plan": plan}, sort_keys=True))
        return 0
    if args.confirmation != "SHARADAR_PROD_WRITE":
        raise SystemExit("--confirmation must be SHARADAR_PROD_WRITE")
    if args.production_confirmation != PRODUCTION_BACKFILL_CONFIRMATION:
        raise SystemExit(
            "--production-confirmation must be BACKFILL_SHARADAR_PROD"
        )

    runtime = connect_mongo_runtime(
        "prod",
        write=True,
        confirmation=args.confirmation,
        production_confirmation=args.production_confirmation,
    )
    try:
        root = runtime.route.artifact_root
        if not root.parent.is_dir():
            raise RuntimeError(f"Sharadar NAS parent is not mounted: {root.parent}")
        client = client_from_vault()
        engine = VendorStorageEngine(
            ArtifactStore(root),
            MongoCurrentStore(runtime.database, batch_size=args.row_batch_size),
            row_batch_size=args.row_batch_size,
        )
        receipts: list[dict[str, object]] = []
        for table in tables:
            receipt = ingest_bulk_table(
                client=client,
                table=table,
                history="full",
                registry=registry,
                storage_engine=engine,
                artifact_root=root,
            )
            receipts.append(
                {
                    "table": table.value,
                    "rows": receipt.storage.parquet.row_count,
                    "raw_sha256": receipt.download.sha256,
                    "parquet_sha256": receipt.storage.parquet.sha256,
                    "run_id": receipt.storage.run_id,
                    "replayed": receipt.storage.replayed,
                }
            )
            print(
                json.dumps(
                    {"status": "TABLE_COMPLETE", "receipt": receipts[-1]},
                    sort_keys=True,
                ),
                flush=True,
            )
        print(
            json.dumps(
                {"status": "PROD_BACKFILL_COMPLETE", "plan": plan, "tables": receipts},
                sort_keys=True,
            )
        )
    finally:
        runtime.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
