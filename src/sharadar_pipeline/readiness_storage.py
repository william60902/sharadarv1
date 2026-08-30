"""Read-only readiness evidence adapter for the vendor storage layout."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .catalog import SharadarTable
from .readiness import (
    FundamentalsCanaryEvidence,
    ReadinessConfigurationError,
    TableEvidence,
    TableGateSpec,
    sha256_file,
)
from .storage.mongo import MongoCurrentStore


class DevStorageEvidenceSource:
    """Inspect committed manifests, artifacts and Mongo current collections.

    The adapter deliberately exposes no mutation operation.  Its ``database``
    dependency only needs the small PyMongo collection surface used below, which
    also makes it usable with an audited read-only Mongo wrapper.
    """

    def __init__(self, database: Any, artifact_root: str | Path) -> None:
        self.database = database
        self.artifact_root = Path(artifact_root).expanduser().resolve()

    def inspect_table(self, spec: TableGateSpec) -> TableEvidence:
        table = spec.table.value
        watermark = self._read_json(
            self.artifact_root / "watermarks" / f"{table}.json"
        )
        run_id = _required_string(watermark, "run_id")
        expected_manifest = (
            self.artifact_root / "runs" / table / f"{run_id}.json"
        ).resolve()
        declared_manifest = _safe_artifact_path(
            self.artifact_root,
            _required_string(watermark, "manifest_path"),
        )
        manifest = self._read_json(expected_manifest)
        published = (
            watermark.get("valid_when_manifest_exists") is True
            and declared_manifest == expected_manifest
            and manifest.get("status") == "published"
            and manifest.get("published") is True
            and manifest.get("run_id") == run_id
            and manifest.get("table") == table
            and watermark.get("table") == table
            and watermark.get("source_sha256")
            == _mapping(manifest, "raw_capture").get("sha256")
            and watermark.get("schema_fingerprint")
            == manifest.get("schema_fingerprint")
            and watermark.get("value") == manifest.get("source_watermark")
        )

        collection = self.database[MongoCurrentStore.collection_name(table)]
        run_filter = {"_storage.run_id": run_id}
        stored_rows = int(collection.count_documents(run_filter))
        null_filter = {
            "$and": [
                run_filter,
                {
                    "$or": [
                        {field: None} for field in spec.primary_key
                    ]
                },
            ]
        }
        null_rows = int(collection.count_documents(null_filter))
        duplicates = _duplicate_primary_keys(collection, spec.primary_key, run_filter)
        pit_missing = _missing_clock_rows(collection, spec.pit_clock_fields, run_filter)
        pit_order = _clock_order_violations(
            collection, spec.pit_clock_order, run_filter
        )

        raw = _mapping(manifest, "raw_capture")
        parquet = _mapping(manifest, "parquet")
        checksum_ok = self._artifact_receipt_valid(raw) and self._artifact_receipt_valid(
            parquet
        )
        replay_ok = (
            manifest.get("replay_verified") is True
            and _deterministic_run_identity(manifest, expected_manifest)
        )
        indexes_ok = _required_indexes_present(
            collection.index_information(), spec.primary_key
        )
        watermark_value = watermark.get("value")
        if isinstance(watermark_value, Mapping):
            watermark_text = json.dumps(
                watermark_value, sort_keys=True, separators=(",", ":")
            )
        elif watermark_value is None:
            watermark_text = None
        else:
            watermark_text = str(watermark_value)

        manifest_rows = manifest.get("row_count")
        if manifest_rows != parquet.get("row_count"):
            manifest_rows = None
        return TableEvidence(
            manifest_id=run_id,
            manifest_published=published,
            manifest_row_count=(
                int(manifest_rows) if type(manifest_rows) is int else None
            ),
            stored_row_count=stored_rows,
            primary_key_null_rows=null_rows,
            duplicate_primary_keys=duplicates,
            actual_schema_digest=_optional_string(manifest, "schema_fingerprint"),
            pit_clock_missing_rows=pit_missing,
            pit_clock_order_violations=pit_order,
            artifact_checksum_verified=checksum_ok,
            replay_verified=replay_ok,
            required_indexes_present=indexes_ok,
            watermark=watermark_text,
            details={"collection": MongoCurrentStore.collection_name(table)},
        )

    def inspect_fundamentals_arq_two_quarter(
        self,
    ) -> FundamentalsCanaryEvidence:
        watermark = self._read_json(
            self.artifact_root / "watermarks" / "fundamentals.json"
        )
        run_id = _required_string(watermark, "run_id")
        collection = self.database[
            MongoCurrentStore.collection_name(SharadarTable.FUNDAMENTALS.value)
        ]
        pipeline = [
            {
                "$match": {
                    "_storage.run_id": run_id,
                    "dimension": "ARQ",
                    "ticker": {"$nin": [None, ""]},
                    "reportperiod": {"$nin": [None, ""]},
                }
            },
            {
                "$group": {
                    "_id": "$ticker",
                    "periods": {"$addToSet": "$reportperiod"},
                }
            },
            {"$match": {"$expr": {"$gte": [{"$size": "$periods"}, 2]}}},
            {"$sort": {"_id": 1}},
            {"$limit": 100},
        ]
        rows = list(collection.aggregate(pipeline, allowDiskUse=False))
        samples = tuple(
            str(row["_id"])
            for row in rows[:10]
            if isinstance(row, Mapping) and row.get("_id")
        )
        return FundamentalsCanaryEvidence(len(rows), samples)

    def _artifact_receipt_valid(self, receipt: Mapping[str, Any]) -> bool:
        sha = receipt.get("sha256")
        byte_count = receipt.get("byte_count")
        artifact_path = receipt.get("artifact_path")
        if (
            not _is_sha256(sha)
            or type(byte_count) is not int
            or byte_count < 0
            or not isinstance(artifact_path, str)
        ):
            return False
        try:
            path = _safe_artifact_path(self.artifact_root, artifact_path)
            return (
                path.is_file()
                and path.stat().st_size == byte_count
                and sha256_file(path) == sha
            )
        except (OSError, ReadinessConfigurationError):
            return False

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ReadinessConfigurationError(
                f"unreadable DEV metadata: {path.name}"
            ) from exc
        if not isinstance(payload, dict):
            raise ReadinessConfigurationError(
                f"DEV metadata must be an object: {path.name}"
            )
        return payload


def _duplicate_primary_keys(
    collection: Any, primary_key: tuple[str, ...], run_filter: Mapping[str, Any]
) -> int:
    group_id = {field: f"${field}" for field in primary_key}
    pipeline = [
        {"$match": dict(run_filter)},
        {"$group": {"_id": group_id, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$count": "duplicate_keys"},
    ]
    rows = list(collection.aggregate(pipeline, allowDiskUse=False))
    return int(rows[0]["duplicate_keys"]) if rows else 0


def _missing_clock_rows(
    collection: Any, fields: tuple[str, ...], run_filter: Mapping[str, Any]
) -> int:
    if not fields:
        return 0
    query = {
        "$and": [dict(run_filter), {"$or": [{field: None} for field in fields]}]
    }
    return int(collection.count_documents(query))


def _clock_order_violations(
    collection: Any,
    order: tuple[tuple[str, str], ...],
    run_filter: Mapping[str, Any],
) -> int:
    if not order:
        return 0
    comparisons = [
        {"$gt": [f"${earlier}", f"${later}"]} for earlier, later in order
    ]
    query = {
        "$and": [
            dict(run_filter),
            {"$expr": {"$or": comparisons}},
        ]
    }
    return int(collection.count_documents(query))


def _required_indexes_present(
    index_information: Mapping[str, Any], primary_key: tuple[str, ...]
) -> bool:
    expected_operational = {("_storage.run_id",), ("_storage.source_sha256",)}
    found: dict[tuple[str, ...], bool] = {}
    for value in index_information.values():
        if not isinstance(value, Mapping):
            continue
        raw_keys = value.get("key", ())
        try:
            keys = tuple(str(item[0]) for item in raw_keys)
        except (TypeError, IndexError):
            continue
        found[keys] = bool(value.get("unique", False))
    return found.get(primary_key) is True and expected_operational.issubset(found)


def _deterministic_run_identity(
    manifest: Mapping[str, Any], manifest_path: Path
) -> bool:
    table = manifest.get("table")
    raw_sha = _mapping(manifest, "raw_capture").get("sha256")
    schema = manifest.get("schema_fingerprint")
    pipeline_version = manifest.get("pipeline_version")
    run_id = manifest.get("run_id")
    if not all(
        isinstance(value, str) and value
        for value in (table, raw_sha, schema, pipeline_version, run_id)
    ):
        return False
    expected = hashlib.sha256(
        f"{table}\0{raw_sha}\0{schema}\0{pipeline_version}".encode()
    ).hexdigest()
    raw_path = _mapping(manifest, "raw_capture").get("artifact_path")
    parquet_path = _mapping(manifest, "parquet").get("artifact_path")
    return (
        run_id == expected
        and manifest_path.stem == run_id
        and _path_contains_digest(raw_path, raw_sha)
        and _path_contains_digest(
            parquet_path, _mapping(manifest, "parquet").get("sha256")
        )
    )


def _path_contains_digest(path: Any, digest: Any) -> bool:
    return (
        isinstance(path, str)
        and _is_sha256(digest)
        and Path(path).stem == digest
    )


def _safe_artifact_path(root: Path, value: str) -> Path:
    path = Path(value)
    resolved = (path if path.is_absolute() else root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        raise ReadinessConfigurationError("artifact path escapes DEV root") from None
    return resolved


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def _required_string(value: Mapping[str, Any], key: str) -> str:
    exact = value.get(key)
    if not isinstance(exact, str) or not exact:
        raise ReadinessConfigurationError(f"DEV metadata is missing {key}")
    return exact


def _optional_string(value: Mapping[str, Any], key: str) -> str | None:
    exact = value.get(key)
    return exact if isinstance(exact, str) else None


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )
