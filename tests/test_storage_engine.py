from __future__ import annotations

import errno
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq

from sharadar_pipeline.schema_registry import load_schema_registry
from sharadar_pipeline.storage import (
    ArtifactStore,
    MongoCurrentStore,
    VendorStorageEngine,
    normalize_row,
    resolve_registry,
)

REGISTRY = {
    "daily": {
        "version": "test.v1",
        "ordered_headers": ("ticker", "date", "value", "count", "note"),
        "primary_key": ("ticker", "date"),
        "types": {
            "ticker": "text",
            "date": "date",
            "value": "double precision",
            "count": "bigint",
            "note": "text",
        },
    }
}


@dataclass
class _BulkResult:
    matched_count: int
    modified_count: int
    upserted_count: int


class _Collection:
    def __init__(self) -> None:
        self.documents = {}
        self.index_calls = []
        self.batch_sizes = []

    def create_index(self, fields, **options):
        self.index_calls.append((tuple(fields), options))
        return options["name"]

    def bulk_write(self, operations, *, ordered):
        assert ordered is False
        self.batch_sizes.append(len(operations))
        matched = modified = upserted = 0
        for operation in operations:
            key = tuple(sorted(operation._filter.items()))
            if key in self.documents:
                matched += 1
                modified += 1
            else:
                upserted += 1
            previous = self.documents.get(key, {})
            self.documents[key] = {
                **previous,
                **operation._doc.get("$setOnInsert", {}),
                **operation._doc["$set"],
            }
        return _BulkResult(matched, modified, upserted)


class _Database:
    def __init__(self) -> None:
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, _Collection())


def test_official_registry_resolves_and_strictly_coerces() -> None:
    registry = load_schema_registry()
    view = resolve_registry(registry, "daily")
    assert view.version == "1"
    assert view.resource_sha256 == registry.resource_sha256
    assert view.primary_keys == ("ticker", "date")

    row = {header: "" for header in view.headers}
    row.update(
        ticker="AAPL",
        date="2026-08-28",
        lastupdated="2026-08-29",
        marketcap="123.5",
        ev="99",
    )
    normalized = normalize_row(row, view)
    assert normalized["date"] == date(2026, 8, 28)
    assert normalized["marketcap"] == 123.5
    assert normalized["ev"] == 99.0
    assert normalized["pe"] is None


def test_raw_capture_is_content_addressed_and_replay_safe(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path)
    first = store.capture_chunks("daily", [b"abc", b"def"], suffix=".zip")
    second = store.capture_chunks("daily", [b"abcdef"], suffix=".zip")

    assert first.artifact_path == second.artifact_path
    assert first.artifact_path.read_bytes() == b"abcdef"
    assert first.replayed is False
    assert second.replayed is True
    assert first.artifact_path.parts[-5:-1] == (
        "raw",
        "daily",
        "sha256",
        first.sha256[:2],
    )


def test_smb_hardlink_fallback_is_atomic_and_replay_safe(
    tmp_path: Path, monkeypatch
) -> None:
    def unsupported_link(source, target):
        raise OSError(errno.ENOTSUP, "SMB hard links unsupported")

    monkeypatch.setattr("sharadar_pipeline.storage.artifacts.os.link", unsupported_link)
    store = ArtifactStore(tmp_path)
    first = store.capture_chunks("daily", [b"smb"], suffix=".zip")
    second = store.capture_chunks("daily", [b"smb"], suffix=".zip")
    manifest = store.write_run_manifest("daily", "smb-run", {"status": "published"})
    same_manifest = store.write_run_manifest(
        "daily", "smb-run", {"status": "published"}
    )

    assert first.replayed is False
    assert second.replayed is True
    assert first.artifact_path.read_bytes() == b"smb"
    assert manifest == same_manifest
    assert json.loads(manifest.read_text()) == {"status": "published"}


def test_mongo_upsert_is_bounded_deduplicated_and_index_cached() -> None:
    database = _Database()
    store = MongoCurrentStore(database, batch_size=2)
    rows = [
        {"ticker": "A", "date": "2026-08-28", "value": "1.5", "count": "2", "note": ""},
        {
            "ticker": "A",
            "date": "2026-08-28",
            "value": "2.5",
            "count": "3",
            "note": "x",
        },
        {"ticker": "B", "date": "2026-08-28", "value": 4, "count": 5, "note": None},
    ]
    receipt = store.upsert_rows(
        "daily", rows, REGISTRY, run_id="run", source_sha256="a" * 64
    )
    store.upsert_rows(
        "daily", rows[-1:], REGISTRY, run_id="run2", source_sha256="b" * 64
    )

    collection = database["normalized_daily_current"]
    assert receipt.input_rows == 3
    assert receipt.submitted_rows == 2
    assert collection.batch_sizes == [1, 1, 1]
    assert len(collection.index_calls) == 3
    a = next(
        value for key, value in collection.documents.items() if ("ticker", "A") in key
    )
    assert a["value"] == 2.5
    assert a["count"] == 3
    assert a["date"] == datetime(2026, 8, 28, tzinfo=UTC)


def test_complete_storage_run_manifest_is_last_and_replay_skips_rows(
    tmp_path: Path,
) -> None:
    artifact_store = ArtifactStore(tmp_path)
    database = _Database()
    engine = VendorStorageEngine(
        artifact_store,
        MongoCurrentStore(database, batch_size=2),
        row_batch_size=2,
    )
    raw = artifact_store.capture_chunks("daily", [b"source bytes"], suffix=".json")
    rows = [
        {"ticker": "A", "date": "2026-08-28", "value": "1.5", "count": "2", "note": ""},
        {"ticker": "B", "date": "2026-08-28", "value": 4.0, "count": 5, "note": "ok"},
        {
            "ticker": "C",
            "date": "2026-08-28",
            "value": None,
            "count": "6",
            "note": None,
        },
    ]
    receipt = engine.ingest_rows(
        "daily", raw, rows, REGISTRY, source_watermark="2026-08-28"
    )

    assert receipt.replayed is False
    assert receipt.parquet.row_count == 3
    parquet = pq.read_table(receipt.parquet.artifact_path)
    assert parquet.num_rows == 3
    assert str(parquet.schema.field("date").type) == "date32[day]"
    assert parquet.column("count").to_pylist() == [2, 5, 6]

    manifest = json.loads(receipt.manifest_path.read_text())
    assert manifest["status"] == "published"
    assert manifest["published"] is True
    assert manifest["row_count"] == 3
    assert manifest["replay_verified"] is True
    assert manifest["raw_capture"]["sha256"] == raw.sha256
    assert manifest["parquet"]["sha256"] == receipt.parquet.sha256
    watermark = json.loads(receipt.watermark_path.read_text())
    assert watermark["table"] == "daily"
    assert watermark["value"] == "2026-08-28"
    assert Path(watermark["manifest_path"]) == receipt.manifest_path
    assert receipt.manifest_path.exists()

    def must_not_iterate():
        raise AssertionError("committed replay must not iterate or rewrite rows")
        yield {}

    replay = engine.ingest_rows(
        "daily", raw, must_not_iterate(), REGISTRY, source_watermark="2026-08-28"
    )
    assert replay.replayed is True
    assert replay.run_id == receipt.run_id
    assert replay.parquet.artifact_path == receipt.parquet.artifact_path
