#!/usr/bin/env python3
"""Write the bounded seven-table paid-data canary to SHARADAR_DEV."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.auth import client_from_vault
from sharadar_pipeline.canary import DEFAULT_CANARY_SYMBOLS, fetch_canary_batches
from sharadar_pipeline.runtime import connect_mongo_runtime
from sharadar_pipeline.schema_registry import load_schema_registry
from sharadar_pipeline.storage import (
    ArtifactStore,
    MongoCurrentStore,
    VendorStorageEngine,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize a bounded real Sharadar canary into SHARADAR_DEV."
    )
    parser.add_argument(
        "--confirmation",
        required=True,
        help="Must be SHARADAR_DEV_WRITE.",
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=datetime.now(UTC).date(),
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_CANARY_SYMBOLS),
    )
    parser.add_argument("--row-batch-size", type=int, default=1_000)
    return parser.parse_args()


def _raw_payload(batch: object) -> bytes:
    query = batch.query.parameters()
    payload = {
        "capture_format": "sharadar.rest-canary/v1",
        "source": "sharadar",
        "table": batch.table.value,
        "query": query,
        "source_watermark": batch.source_watermark,
        "data": list(batch.rows),
    }
    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
        + b"\n"
    )


def main() -> int:
    args = parse_args()
    runtime = connect_mongo_runtime(
        "dev",
        write=True,
        confirmation=args.confirmation,
    )
    try:
        root = runtime.route.artifact_root
        if not root.parent.is_dir():
            raise RuntimeError(f"Sharadar NAS parent is not mounted: {root.parent}")
        root.mkdir(parents=True, exist_ok=True)
        registry = load_schema_registry()
        artifact_store = ArtifactStore(root)
        engine = VendorStorageEngine(
            artifact_store,
            MongoCurrentStore(runtime.database, batch_size=args.row_batch_size),
            row_batch_size=args.row_batch_size,
        )
        client = client_from_vault()
        batches = fetch_canary_batches(
            client,
            as_of=args.as_of,
            symbols=tuple(args.symbols),
        )

        summaries: list[dict[str, object]] = []
        for batch in batches:
            schema = registry.table(batch.table)
            expected = set(schema.ordered_headers)
            if any(set(row) != expected for row in batch.rows):
                raise RuntimeError(
                    f"{batch.table.value}: REST keys differ from pinned schema"
                )
            raw = artifact_store.capture_chunks(
                batch.table.value,
                (_raw_payload(batch),),
                suffix=".json",
            )
            watermark = {
                "mode": "rest_canary",
                "value": batch.source_watermark,
                "as_of": args.as_of.isoformat(),
            }
            receipt = engine.ingest_rows(
                batch.table.value,
                raw,
                batch.rows,
                registry,
                source_watermark=watermark,
                pipeline_version="dev-canary-v1",
            )
            replay = engine.ingest_rows(
                batch.table.value,
                raw,
                batch.rows,
                registry,
                source_watermark=watermark,
                pipeline_version="dev-canary-v1",
            )
            if not replay.replayed or replay.run_id != receipt.run_id:
                raise RuntimeError(f"{batch.table.value}: replay was not idempotent")
            summaries.append(
                {
                    "table": batch.table.value,
                    "rows": receipt.parquet.row_count,
                    "mongo_collection": receipt.mongo.collection,
                    "run_id": receipt.run_id,
                    "raw_sha256": receipt.raw_capture.sha256,
                    "parquet_sha256": receipt.parquet.sha256,
                    "replay_verified": True,
                }
            )

        print(
            json.dumps(
                {
                    "status": "OK",
                    "database": runtime.route.database_name,
                    "artifact_root": str(root),
                    "schema_registry_sha256": registry.resource_sha256,
                    "tables": summaries,
                },
                sort_keys=True,
                ensure_ascii=False,
            )
        )
    finally:
        runtime.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
