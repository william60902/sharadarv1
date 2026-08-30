"""Idempotent orchestration across raw, Parquet, Mongo, and run metadata."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..catalog import normalize_table
from .artifacts import (
    ArtifactStore,
    ParquetArtifactReceipt,
    RawCaptureReceipt,
    receipt_dict,
    resolve_registry,
)
from .mongo import MongoCurrentStore, MongoUpsertReceipt


@dataclass(frozen=True, slots=True)
class StorageRunReceipt:
    run_id: str
    table: str
    raw_capture: RawCaptureReceipt
    parquet: ParquetArtifactReceipt
    mongo: MongoUpsertReceipt
    watermark_path: Path | None
    manifest_path: Path
    replayed: bool


class VendorStorageEngine:
    """Commit normalized vendor rows; the immutable run manifest is written last."""

    def __init__(
        self,
        artifact_store: ArtifactStore,
        mongo_store: MongoCurrentStore,
        *,
        row_batch_size: int = 10_000,
    ) -> None:
        if (
            not isinstance(row_batch_size, int)
            or isinstance(row_batch_size, bool)
            or row_batch_size <= 0
        ):
            raise ValueError("row_batch_size must be a positive integer")
        self.artifact_store = artifact_store
        self.mongo_store = mongo_store
        self.row_batch_size = row_batch_size

    def ingest_rows(
        self,
        table: str,
        raw_capture: RawCaptureReceipt,
        rows: Iterable[Mapping[str, Any]],
        registry: Any,
        *,
        source_watermark: Mapping[str, Any] | str | float | None = None,
        pipeline_version: str = "1",
    ) -> StorageRunReceipt:
        exact_table = normalize_table(table).value
        if raw_capture.table != exact_table:
            raise ValueError("raw capture table does not match ingestion table")
        view = resolve_registry(registry, exact_table)
        run_id = _run_id(
            exact_table, raw_capture.sha256, view.fingerprint, pipeline_version
        )
        existing = self.artifact_store.read_run_manifest(exact_table, run_id)
        if existing is not None:
            return _receipt_from_manifest(
                existing,
                raw_capture,
                self.artifact_store.run_manifest_path(exact_table, run_id),
            )

        parquet_writer = self.artifact_store.parquet_writer(
            exact_table,
            registry,
            run_id,
            row_group_size=self.row_batch_size,
        )
        mongo_totals: list[MongoUpsertReceipt] = []
        batch: list[Mapping[str, Any]] = []
        try:
            for row in rows:
                batch.append(row)
                if len(batch) >= self.row_batch_size:
                    parquet_writer.write_rows(batch)
                    mongo_totals.append(
                        self.mongo_store.upsert_rows(
                            exact_table,
                            batch,
                            registry,
                            run_id=run_id,
                            source_sha256=raw_capture.sha256,
                        )
                    )
                    batch = []
            if batch:
                parquet_writer.write_rows(batch)
                mongo_totals.append(
                    self.mongo_store.upsert_rows(
                        exact_table,
                        batch,
                        registry,
                        run_id=run_id,
                        source_sha256=raw_capture.sha256,
                    )
                )
            parquet = parquet_writer.commit()
        except BaseException:
            parquet_writer.abort()
            raise
        mongo = _sum_mongo(
            exact_table, self.mongo_store.collection_name(exact_table), mongo_totals
        )
        if mongo.input_rows != parquet.row_count:
            raise RuntimeError("Mongo and Parquet normalized row counts diverged")

        completed_at = datetime.now(UTC).isoformat()
        watermark_value = _watermark_value(source_watermark)
        watermark_path = None
        if source_watermark is not None:
            watermark_payload = {
                "version": 1,
                "source": "sharadar",
                "table": exact_table,
                "value": watermark_value,
                "run_id": run_id,
                "source_sha256": raw_capture.sha256,
                "schema_fingerprint": view.fingerprint,
                "completed_at": completed_at,
                "manifest_path": str(
                    self.artifact_store.run_manifest_path(exact_table, run_id)
                ),
                "valid_when_manifest_exists": True,
            }
            watermark_path = self.artifact_store.write_watermark(
                exact_table, watermark_payload
            )

        manifest_payload = {
            "manifest_version": 1,
            "source": "sharadar",
            "layer": "vendor",
            "status": "published",
            "published": True,
            "table": exact_table,
            "run_id": run_id,
            "pipeline_version": pipeline_version,
            "schema_version": view.version,
            "schema_fingerprint": view.fingerprint,
            "registry_resource_sha256": view.resource_sha256,
            "completed_at": completed_at,
            "row_count": parquet.row_count,
            "replay_verified": True,
            "raw_capture": receipt_dict(raw_capture),
            "parquet": receipt_dict(parquet),
            "mongo": asdict(mongo),
            "source_watermark": watermark_value,
        }
        # This immutable file is the sole commit marker and must remain last.
        manifest_path = self.artifact_store.write_run_manifest(
            exact_table, run_id, manifest_payload
        )
        return StorageRunReceipt(
            run_id=run_id,
            table=exact_table,
            raw_capture=raw_capture,
            parquet=parquet,
            mongo=mongo,
            watermark_path=watermark_path,
            manifest_path=manifest_path,
            replayed=False,
        )


def _run_id(
    table: str, raw_sha256: str, schema_fingerprint: str, pipeline_version: str
) -> str:
    if type(pipeline_version) is not str or not pipeline_version:
        raise ValueError("pipeline_version must be a non-empty string")
    value = f"{table}\0{raw_sha256}\0{schema_fingerprint}\0{pipeline_version}".encode()
    return hashlib.sha256(value).hexdigest()


def _watermark_value(value: Mapping[str, Any] | str | float | None) -> Any:
    if value is None:
        return None
    if isinstance(value, Mapping):
        if "value" in value:
            return value["value"]
        return dict(value)
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        return value
    raise TypeError("source_watermark must be a scalar or mapping")


def _sum_mongo(
    table: str, collection: str, receipts: list[MongoUpsertReceipt]
) -> MongoUpsertReceipt:
    fields = (
        "input_rows",
        "submitted_rows",
        "matched_count",
        "modified_count",
        "upserted_count",
        "batches",
    )
    values = {
        field: sum(getattr(receipt, field) for receipt in receipts) for field in fields
    }
    return MongoUpsertReceipt(table=table, collection=collection, **values)


def _receipt_from_manifest(
    manifest: Mapping[str, Any], raw_capture: RawCaptureReceipt, manifest_path: Path
) -> StorageRunReceipt:
    parquet_value = manifest["parquet"]
    mongo_value = manifest["mongo"]
    parquet = ParquetArtifactReceipt(
        table=str(parquet_value["table"]),
        sha256=str(parquet_value["sha256"]),
        byte_count=int(parquet_value["byte_count"]),
        row_count=int(parquet_value["row_count"]),
        artifact_path=Path(parquet_value["artifact_path"]),
        replayed=True,
    )
    mongo = MongoUpsertReceipt(**mongo_value)
    watermark = manifest_path.parents[2] / "watermarks" / f"{manifest['table']}.json"
    return StorageRunReceipt(
        run_id=str(manifest["run_id"]),
        table=str(manifest["table"]),
        raw_capture=raw_capture,
        parquet=parquet,
        mongo=mongo,
        watermark_path=watermark if watermark.exists() else None,
        manifest_path=manifest_path,
        replayed=True,
    )
