from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sharadar_pipeline.catalog import SharadarTable
from sharadar_pipeline.readiness import (
    FundamentalsCanaryEvidence,
    ReadinessConfigurationError,
    TableEvidence,
    TableGateSpec,
    fundamentals_plan_tables,
    gate_specs_from_registry,
    sha256_file,
    verify_dev_readiness,
)
from sharadar_pipeline.readiness_storage import DevStorageEvidenceSource
from sharadar_pipeline.routes import Deployment, route_for
from sharadar_pipeline.schema_registry import load_schema_registry

SCHEMA_DIGEST = "a" * 64


def gate_specs() -> tuple[TableGateSpec, ...]:
    return tuple(
        TableGateSpec(
            table=table,
            primary_key=("id",),
            schema_digest=SCHEMA_DIGEST,
            pit_clock_fields=("date",) if table is SharadarTable.FUNDAMENTALS else (),
            required_indexes=(("id",),),
        )
        for table in fundamentals_plan_tables()
    )


GOOD_EVIDENCE = TableEvidence(
    manifest_id="run-1",
    manifest_published=True,
    manifest_row_count=2,
    stored_row_count=2,
    primary_key_null_rows=0,
    duplicate_primary_keys=0,
    actual_schema_digest=SCHEMA_DIGEST,
    pit_clock_missing_rows=0,
    pit_clock_order_violations=0,
    artifact_checksum_verified=True,
    replay_verified=True,
    required_indexes_present=True,
    watermark="2026-08-30T00:00:00Z",
)


class FakeSource:
    def __init__(self, *, overrides=None, canary_count: int = 2):
        self.overrides = overrides or {}
        self.canary_count = canary_count

    def inspect_table(self, spec):
        return self.overrides.get(spec.table, GOOD_EVIDENCE)

    def inspect_fundamentals_arq_two_quarter(self):
        return FundamentalsCanaryEvidence(self.canary_count, ("AAPL", "MSFT"))


def test_complete_dev_evidence_is_ready_but_never_authorizes_prod_write():
    report = verify_dev_readiness(
        FakeSource(),
        gate_specs(),
        checked_at=datetime(2026, 8, 31, tzinfo=UTC),
    )

    assert report.ready_for_prod_backfill is True
    assert report.prod_write_authorized is False
    assert report.database_name == "SHARADAR_DEV"
    payload = json.loads(report.to_json())
    assert payload["format"] == "sharadar.dev-readiness/v1"
    assert payload["checked_at"] == "2026-08-31T00:00:00Z"


@pytest.mark.parametrize(
    ("field", "bad_value", "failed_name"),
    [
        ("stored_row_count", 0, "nonempty_rows"),
        ("primary_key_null_rows", 1, "primary_key_not_null"),
        ("duplicate_primary_keys", 1, "primary_key_unique"),
        ("actual_schema_digest", "b" * 64, "schema_digest"),
        ("pit_clock_order_violations", 1, "pit_clock_order"),
        ("artifact_checksum_verified", False, "artifact_checksum"),
        ("replay_verified", False, "replay_idempotent"),
        ("required_indexes_present", False, "required_indexes"),
        ("watermark", None, "watermark"),
    ],
)
def test_any_material_table_failure_blocks_readiness(field, bad_value, failed_name):
    broken = replace(GOOD_EVIDENCE, **{field: bad_value})
    report = verify_dev_readiness(
        FakeSource(overrides={SharadarTable.FUNDAMENTALS: broken}), gate_specs()
    )

    assert report.ready_for_prod_backfill is False
    assert any(
        check.name == failed_name
        and check.table == "fundamentals"
        and not check.passed
        for check in report.checks
    )


def test_arq_canary_must_have_two_distinct_quarters_for_at_least_one_issuer():
    report = verify_dev_readiness(FakeSource(canary_count=0), gate_specs())

    assert report.ready_for_prod_backfill is False
    assert any(
        check.name == "fundamentals_arq_two_quarter_canary" and not check.passed
        for check in report.checks
    )


def test_gate_rejects_prod_route_and_incomplete_table_contract():
    with pytest.raises(ReadinessConfigurationError, match="only.*SHARADAR_DEV"):
        verify_dev_readiness(
            FakeSource(), gate_specs(), route=route_for(Deployment.PROD)
        )
    with pytest.raises(ReadinessConfigurationError, match="seven"):
        verify_dev_readiness(FakeSource(), gate_specs()[:-1])


def test_sha256_file_streams_expected_bytes(tmp_path):
    artifact = tmp_path / "artifact.zip"
    artifact.write_bytes(b"abc" * 100_000)

    assert sha256_file(artifact, chunk_size=4096) == hashlib.sha256(
        artifact.read_bytes()
    ).hexdigest()


def test_active_registry_adapts_to_all_seven_storage_fingerprints():
    specs = gate_specs_from_registry(load_schema_registry())

    assert tuple(spec.table for spec in specs) == fundamentals_plan_tables()
    assert all(len(spec.schema_digest) == 64 for spec in specs)
    assert all(spec.primary_key in spec.required_indexes for spec in specs)


class _ProbeCollection:
    def __init__(self):
        self._count_results = iter((2, 0))

    def count_documents(self, query):
        return next(self._count_results)

    def aggregate(self, pipeline, *, allowDiskUse):
        assert allowDiskUse is False
        return []

    def index_information(self):
        return {
            "pk": {"key": [("table", 1), ("indicator", 1)], "unique": True},
            "run": {"key": [("_storage.run_id", 1)]},
            "source": {"key": [("_storage.source_sha256", 1)]},
        }


class _ProbeDatabase:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, name):
        assert name == "descriptions"
        return self.collection


def test_storage_adapter_reads_exact_manifest_last_contract(tmp_path: Path):
    spec = gate_specs_from_registry(load_schema_registry())[0]
    raw = b"raw"
    parquet = b"parquet"
    raw_sha = hashlib.sha256(raw).hexdigest()
    parquet_sha = hashlib.sha256(parquet).hexdigest()
    pipeline_version = "dev-canary-v1"
    run_id = hashlib.sha256(
        f"descriptions\0{raw_sha}\0{spec.schema_digest}\0{pipeline_version}".encode()
    ).hexdigest()
    raw_path = tmp_path / "raw" / "descriptions" / f"{raw_sha}.json"
    parquet_path = (
        tmp_path / "normalized" / "descriptions" / f"{parquet_sha}.parquet"
    )
    raw_path.parent.mkdir(parents=True)
    parquet_path.parent.mkdir(parents=True)
    raw_path.write_bytes(raw)
    parquet_path.write_bytes(parquet)
    manifest_path = tmp_path / "runs" / "descriptions" / f"{run_id}.json"
    manifest_path.parent.mkdir(parents=True)
    manifest = {
        "status": "published",
        "published": True,
        "table": "descriptions",
        "run_id": run_id,
        "pipeline_version": pipeline_version,
        "schema_fingerprint": spec.schema_digest,
        "row_count": 2,
        "replay_verified": True,
        "source_watermark": "2026-08-31",
        "raw_capture": {
            "sha256": raw_sha,
            "byte_count": len(raw),
            "artifact_path": str(raw_path),
        },
        "parquet": {
            "sha256": parquet_sha,
            "byte_count": len(parquet),
            "row_count": 2,
            "artifact_path": str(parquet_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest))
    watermark_path = tmp_path / "watermarks" / "descriptions.json"
    watermark_path.parent.mkdir()
    watermark_path.write_text(
        json.dumps(
            {
                "table": "descriptions",
                "value": "2026-08-31",
                "run_id": run_id,
                "source_sha256": raw_sha,
                "schema_fingerprint": spec.schema_digest,
                "manifest_path": str(manifest_path),
                "valid_when_manifest_exists": True,
            }
        )
    )

    source = DevStorageEvidenceSource(
        _ProbeDatabase(_ProbeCollection()), tmp_path
    )
    evidence = source.inspect_table(spec)

    assert evidence.manifest_published is True
    assert evidence.manifest_row_count == evidence.stored_row_count == 2
    assert evidence.artifact_checksum_verified is True
    assert evidence.replay_verified is True
    assert evidence.required_indexes_present is True
