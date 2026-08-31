#!/usr/bin/env python3
"""Bounded operational health check for the resident SHARADAR_PROD service."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.runtime import connect_mongo_runtime
from sharadar_pipeline.routes import route_for
from sharadar_pipeline.schema_registry import FUNDAMENTALS_TABLES, load_schema_registry
from sharadar_pipeline.storage import resolve_registry

EASTERN = ZoneInfo("America/New_York")
PROD_ROOT = route_for("prod").artifact_root
PROD_MOUNT = PROD_ROOT.parents[1]
DAILY_STATE = REPO_ROOT / "var" / "state" / "prod_daily.json"
MONTHLY_STATE = REPO_ROOT / "var" / "state" / "prod_monthly.json"
DEFAULT_OUTPUT = REPO_ROOT / "var" / "health" / "latest.json"
MIRROR_OUTPUT = PROD_ROOT / "health" / "latest.json"
DAILY_LABEL = "com.medina.sharadar-prod-daily"
MONTHLY_LABEL = "com.medina.sharadar-prod-monthly"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check SHARADAR_PROD operations.")
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _progress(stage: str) -> None:
    print(f"health-stage={stage}", file=sys.stderr, flush=True)


def expected_daily_service_date(now: datetime) -> str:
    eastern = now.astimezone(EASTERN)
    expected = eastern.date()
    if eastern.timetz().replace(tzinfo=None) < time(0, 45):
        expected -= timedelta(days=1)
    return expected.isoformat()


def expected_monthly_service_month(now: datetime) -> str:
    eastern = now.astimezone(EASTERN)
    current = eastern.date().replace(day=1)
    if eastern.day < 2 or (
        eastern.day == 2 and eastern.timetz().replace(tzinfo=None) < time(3, 15)
    ):
        current -= timedelta(days=1)
    return current.strftime("%Y-%m")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _is_loaded(label: str) -> bool:
    if sys.platform != "darwin":
        marker = {
            DAILY_LABEL: "scripts/run_prod_daily.py",
            MONTHLY_LABEL: "scripts/run_prod_monthly.py",
        }.get(label)
        if marker is None:
            return False
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, check=False
        )
        return result.returncode == 0 and marker in result.stdout
    result = subprocess.run(
        ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _path_inside(root: Path, value: object) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        declared = Path(value).expanduser()
        path = declared.resolve()
        if path.is_relative_to(root.resolve()):
            return path
        parts = declared.parts
        for index in range(len(parts) - 1):
            if parts[index : index + 2] == ("sharadar", "prod"):
                return root.joinpath(*parts[index + 2 :]).resolve()
        return None
    except OSError:
        return None


def _receipt_exists(root: Path, value: object) -> bool:
    if not isinstance(value, dict):
        return False
    path = _path_inside(root, value.get("artifact_path"))
    expected = value.get("byte_count")
    return (
        path is not None
        and type(expected) is int
        and path.is_file()
        and path.stat().st_size == expected
    )


def _has_unique_pk(indexes: dict[str, Any], expected: tuple[str, ...]) -> bool:
    return any(
        metadata.get("unique") is True
        and tuple(field for field, _direction in metadata.get("key", [])) == expected
        for metadata in indexes.values()
    )


def build_report(now: datetime | None = None) -> dict[str, Any]:
    _progress("build-start")
    checked_at = now or datetime.now(UTC)
    daily_state = _read_json(DAILY_STATE)
    monthly_state = _read_json(MONTHLY_STATE)
    checks: dict[str, bool] = {
        "nas_mounted": os.path.ismount(PROD_MOUNT),
        "prod_artifact_root": PROD_ROOT.is_dir(),
        "daily_scheduler_loaded": _is_loaded(DAILY_LABEL),
        "monthly_scheduler_loaded": _is_loaded(MONTHLY_LABEL),
        "daily_service_current": (
            daily_state.get("last_success_service_date")
            == expected_daily_service_date(checked_at)
        ),
        "monthly_reconciliation_current": (
            monthly_state.get("last_success_month")
            == expected_monthly_service_month(checked_at)
        ),
    }
    table_results: list[dict[str, Any]] = []
    database_name: str | None = None
    mongo_error: str | None = None

    try:
        _progress("mongo-connect")
        runtime = connect_mongo_runtime("prod", write=False)
        try:
            _progress("mongo-connected")
            database_name = runtime.database.name
            registry = load_schema_registry()
            expected_tables = [table.value for table in FUNDAMENTALS_TABLES]
            actual_tables = sorted(runtime.database.list_collection_names())
            checks["mongo_collection_set"] = set(actual_tables) == set(expected_tables)
            for table in expected_tables:
                _progress(f"table-{table}")
                schema = registry.table(table)
                storage_schema = resolve_registry(registry, table)
                watermark = _read_json(PROD_ROOT / "watermarks" / f"{table}.json")
                run_id = watermark.get("run_id")
                manifest_path = (
                    PROD_ROOT / "runs" / table / f"{run_id}.json"
                    if isinstance(run_id, str) and run_id
                    else Path("/__missing_manifest__")
                )
                manifest = _read_json(manifest_path)
                raw_receipt_ok = _receipt_exists(
                    PROD_ROOT, manifest.get("raw_capture")
                )
                parquet_receipt_ok = _receipt_exists(
                    PROD_ROOT, manifest.get("parquet")
                )
                mongo_nonempty = (
                    runtime.database[table].estimated_document_count() > 0
                )
                primary_key_index = _has_unique_pk(
                    runtime.database[table].index_information(),
                    tuple(schema.primary_key),
                )
                table_checks = {
                    "watermark_manifest": (
                        manifest_path.is_file()
                        and manifest.get("published") is True
                        and manifest.get("status") == "published"
                        and manifest.get("run_id") == run_id
                        and manifest.get("table") == table
                    ),
                    "schema_fingerprint": (
                        manifest.get("schema_fingerprint")
                        == storage_schema.fingerprint
                        == watermark.get("schema_fingerprint")
                    ),
                    "raw_receipt": raw_receipt_ok,
                    "parquet_receipt": parquet_receipt_ok,
                    "mongo_nonempty": mongo_nonempty,
                    "primary_key_index": primary_key_index,
                }
                table_results.append(
                    {
                        "table": table,
                        "run_id": run_id,
                        "watermark": watermark.get("value"),
                        "checks": table_checks,
                        "passed": all(table_checks.values()),
                    }
                )
        finally:
            runtime.close()
    except Exception as error:
        checks["mongo_connection"] = False
        mongo_error = f"{type(error).__name__}: {error}"
    else:
        checks["mongo_connection"] = True

    passed = all(checks.values()) and all(item["passed"] for item in table_results)
    _progress("build-complete")
    return {
        "format": "sharadar.prod-health/v1",
        "checked_at": checked_at.isoformat().replace("+00:00", "Z"),
        "status": "PASS" if passed else "FAIL",
        "database": database_name,
        "artifact_root": str(PROD_ROOT),
        "expected_daily_service_date": expected_daily_service_date(checked_at),
        "expected_monthly_service_month": expected_monthly_service_month(checked_at),
        "scheduler_state": {"daily": daily_state, "monthly": monthly_state},
        "checks": checks,
        "tables": table_results,
        "mongo_error": mongo_error,
    }


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _notify_transition(previous: str | None, current: str) -> None:
    if previous == current or (previous is None and current == "PASS"):
        return
    message = (
        "Sharadar PROD health check failed. Inspect prod_health.err.log."
        if current == "FAIL"
        else "Sharadar PROD health recovered."
    )
    command = (
        [
            "osascript",
            "-e",
            f'display notification "{message}" with title "Medina Sharadar"',
        ]
        if sys.platform == "darwin"
        else ["logger", "-t", "medina-sharadar-health", message]
    )
    subprocess.run(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def main() -> int:
    args = parse_args()
    previous = _read_json(args.output).get("status")
    report = build_report()
    _progress("write-local")
    _write_report(args.output, report)
    if os.path.ismount(PROD_MOUNT):
        _progress("write-nas")
        _write_report(MIRROR_OUTPUT, report)
    if args.notify:
        _progress("notify")
        _notify_transition(previous if isinstance(previous, str) else None, report["status"])
    rendered = json.dumps(
        report,
        ensure_ascii=False,
        indent=None if args.compact else 2,
        sort_keys=True,
    )
    print(rendered, flush=True)
    _progress("complete")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
