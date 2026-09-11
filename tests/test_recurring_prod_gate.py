import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import pytest


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


VERIFY = load("verify_prod_baseline")
BACKFILL = load("backfill_prod")
MONTHLY = load("run_prod_monthly")


def baseline():
    checks = {
        k: True
        for k in [
            "baseline_published_manifest",
            "schema_fingerprint",
            "mongo_contains_baseline",
            "parquet_matches_manifest",
            "raw_size",
            "parquet_size",
            "primary_key_index",
            "current_watermark_published",
        ]
    }
    return {
        "format": "sharadar.prod-baseline-readback/v1",
        "database": "SHARADAR_PROD",
        "status": "PASS",
        "collection_set_ok": True,
        "tables": [
            {"table": "daily", "run_id": "a" * 64, "passed": True, "checks": checks}
        ],
    }


def test_pinned_baseline_required_and_no_fallback(tmp_path):
    path = tmp_path / "baseline.json"
    with pytest.raises(FileNotFoundError):
        VERIFY._strict_baseline_run_ids(path, ["daily"])
    path.write_text(json.dumps(baseline()))
    assert VERIFY._strict_baseline_run_ids(path, ["daily"]) == {"daily": "a" * 64}
    with pytest.raises(ValueError):
        VERIFY._strict_baseline_run_ids(path, ["daily", "actions"])


@pytest.mark.parametrize(
    "mutation", ["wrong_db", "failed", "duplicate", "bad_run", "missing_check"]
)
def test_reject_unapproved_baseline(tmp_path, mutation):
    value = baseline()
    if mutation == "wrong_db":
        value["database"] = "SHARADAR_DEV"
    if mutation == "failed":
        value["status"] = "FAIL"
    if mutation == "duplicate":
        value["tables"] *= 2
    if mutation == "bad_run":
        value["tables"][0]["run_id"] = "../x"
    if mutation == "missing_check":
        value["tables"][0]["checks"].pop("schema_fingerprint")
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        VERIFY._strict_baseline_run_ids(path, ["daily"])


def test_monthly_uses_explicit_recurring_gate_and_keeps_confirmations():
    command = MONTHLY._backfill_command()
    assert "--recurring-prod" in command
    assert "SHARADAR_PROD_WRITE" in command
    assert "BACKFILL_SHARADAR_PROD" in command


@pytest.mark.parametrize("recurring", [False, True])
def test_gate_routing_before_any_write(monkeypatch, recurring):
    monkeypatch.setattr(
        BACKFILL,
        "parse_args",
        lambda: SimpleNamespace(recurring_prod=recurring, table=None, execute=False),
    )
    calls = []
    monkeypatch.setattr(
        BACKFILL, "_verify_live_dev", lambda registry: calls.append("dev") or {}
    )
    monkeypatch.setattr(
        BACKFILL, "_verify_recurring_prod", lambda: calls.append("prod") or {}
    )
    monkeypatch.setattr(
        BACKFILL,
        "connect_mongo_runtime",
        lambda *a, **k: pytest.fail("no writer in plan"),
    )
    assert BACKFILL.main() == 0
    assert calls == (["prod"] if recurring else ["dev"])


def test_failed_prod_readback_blocks(monkeypatch):
    monkeypatch.setattr(
        BACKFILL.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=1)
    )
    with pytest.raises(RuntimeError, match="verification failed"):
        BACKFILL._verify_recurring_prod()


def test_successful_readback_is_required_not_just_exit_zero(monkeypatch):
    report = {
        **baseline(),
        "checked_at": "2026-09-12T00:00:00Z",
        "baseline_report_sha256": "b" * 64,
    }

    def run(command, **kwargs):
        assert "--require-pinned-baseline" in command
        assert "--output" not in command
        assert kwargs["timeout"] == 120
        return SimpleNamespace(returncode=0, stdout=json.dumps(report))

    monkeypatch.setattr(BACKFILL.subprocess, "run", run)
    assert BACKFILL._verify_recurring_prod()["prod_write_authorized"] is False
    report["status"] = "FAIL"
    with pytest.raises(RuntimeError, match="receipt is invalid"):
        BACKFILL._verify_recurring_prod()
