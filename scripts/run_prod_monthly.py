#!/usr/bin/env python3
"""Run one full SHARADAR_PROD bulk reconciliation per Eastern month."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.routes import route_for

LABEL = "com.medina.sharadar-prod-monthly"
EASTERN = ZoneInfo("America/New_York")
DEFAULT_CUTOFF = time(3, 15)
DEFAULT_MINIMUM_DAY = 2
RUNTIME_ROOT = REPO_ROOT / "var"
STATE_PATH = RUNTIME_ROOT / "state" / "prod_monthly.json"
LOCK_PATH = RUNTIME_ROOT / "run" / "prod_ingestion.lock"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one guarded monthly full PROD bulk reconciliation."
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument(
        "--initialize-current-baseline",
        action="store_true",
        help="Mark the current month complete from the existing PASS baseline report.",
    )
    return parser.parse_args()


def _emit(status: str, **values: object) -> None:
    print(
        json.dumps(
            {
                "status": status,
                "scheduler": LABEL,
                "timestamp_utc": datetime.now(UTC).isoformat(),
                **values,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _write_state(payload: dict[str, object]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(STATE_PATH)


def monthly_decision(
    now: datetime,
    state: dict[str, object],
    *,
    cutoff: time = DEFAULT_CUTOFF,
    minimum_day: int = DEFAULT_MINIMUM_DAY,
    force: bool = False,
) -> tuple[bool, str, str]:
    now_eastern = now.astimezone(EASTERN)
    service_month = now_eastern.strftime("%Y-%m")
    if force:
        return True, "forced", service_month
    if now_eastern.day < minimum_day or (
        now_eastern.day == minimum_day
        and now_eastern.timetz().replace(tzinfo=None) < cutoff
    ):
        return False, "before_monthly_window", service_month
    if state.get("last_success_month") == service_month:
        return False, "already_complete", service_month
    return True, "due", service_month


def _require_prod_nas() -> Path:
    root = route_for("prod").artifact_root
    mount = root.parents[1]
    if not os.path.ismount(mount):
        raise RuntimeError(f"required NAS is not mounted: {mount}")
    if not root.is_dir():
        raise RuntimeError(f"PROD artifact root is unavailable: {root}")
    return root


@contextmanager
def _exclusive_lock() -> Iterator[None]:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another SHARADAR_PROD ingestion is running") from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _backfill_command() -> list[str]:
    return [
        str(REPO_ROOT / "venv" / "bin" / "python"),
        str(REPO_ROOT / "scripts" / "backfill_prod.py"),
        "--execute",
        "--confirmation",
        "SHARADAR_PROD_WRITE",
        "--production-confirmation",
        "BACKFILL_SHARADAR_PROD",
    ]


def main() -> int:
    args = parse_args()
    now = datetime.now(UTC)
    root = _require_prod_nas()
    state = _read_json(STATE_PATH)
    should_run, reason, service_month = monthly_decision(
        now, state, force=args.force
    )

    if args.initialize_current_baseline:
        baseline = _read_json(root / "readiness" / "latest.json")
        if baseline.get("status") != "PASS":
            _emit("RETRY_REQUIRED", reason="baseline_report_not_pass")
            return 1
        _write_state(
            {
                "version": 1,
                "scheduler": LABEL,
                "last_success_month": service_month,
                "last_success_finished_at": now.isoformat(),
                "source": "initial_full_history_baseline",
                "artifact_root": str(root),
            }
        )
        _emit("INITIALIZED", service_month=service_month)
        return 0

    if not should_run:
        _emit("SKIPPED", reason=reason, service_month=service_month)
        return 0
    if args.check_only:
        _emit(
            "READY",
            reason=reason,
            service_month=service_month,
            artifact_root=str(root),
        )
        return 0

    try:
        with _exclusive_lock():
            started_at = datetime.now(UTC)
            _emit("STARTED", reason=reason, service_month=service_month)
            result = subprocess.run(_backfill_command(), cwd=REPO_ROOT, check=False)
            finished_at = datetime.now(UTC)
            if result.returncode != 0:
                _emit(
                    "RETRY_REQUIRED",
                    reason="bulk_reconciliation_failed",
                    service_month=service_month,
                    returncode=result.returncode,
                )
                return result.returncode or 1
            _write_state(
                {
                    "version": 1,
                    "scheduler": LABEL,
                    "last_success_month": service_month,
                    "last_success_started_at": started_at.isoformat(),
                    "last_success_finished_at": finished_at.isoformat(),
                    "source": "full_history_bulk_reconciliation",
                    "artifact_root": str(root),
                }
            )
            _emit("COMPLETE", service_month=service_month)
            return 0
    except RuntimeError as error:
        _emit("RETRY_REQUIRED", reason="lock_unavailable", error=str(error))
        return 75


if __name__ == "__main__":
    raise SystemExit(main())
