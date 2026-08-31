#!/usr/bin/env python3
"""Run the SHARADAR_PROD incremental update once per US/Eastern service day.

The host scheduler invokes this entry point hourly. The script waits until the configured
Eastern-time cutoff, refuses to write when the NAS is not mounted, prevents
overlap with an existing run, and records success locally so retries stop after
the first successful run of the day.
"""

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

LABEL = "com.medina.sharadar-prod-daily"
EASTERN = ZoneInfo("America/New_York")
DEFAULT_CUTOFF = time(0, 45)
RUNTIME_ROOT = REPO_ROOT / "var"
STATE_PATH = RUNTIME_ROOT / "state" / "prod_daily.json"
LOCK_PATH = RUNTIME_ROOT / "run" / "prod_ingestion.lock"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one guarded SHARADAR_PROD daily incremental update."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore the cutoff and prior-success gate; mount and lock checks remain.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Evaluate the schedule and prerequisites without starting an update.",
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


def _read_state(path: Path = STATE_PATH) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _write_state(payload: dict[str, object], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def schedule_decision(
    now: datetime,
    state: dict[str, object],
    *,
    cutoff: time = DEFAULT_CUTOFF,
    force: bool = False,
) -> tuple[bool, str, str]:
    """Return ``(should_run, reason, Eastern service date)``."""

    now_eastern = now.astimezone(EASTERN)
    service_date = now_eastern.date().isoformat()
    if force:
        return True, "forced", service_date
    if now_eastern.timetz().replace(tzinfo=None) < cutoff:
        return False, "before_cutoff", service_date
    if state.get("last_success_service_date") == service_date:
        return False, "already_complete", service_date
    return True, "due", service_date


def _require_prod_nas() -> Path:
    root = route_for("prod").artifact_root
    mount = root.parents[1]
    if not os.path.ismount(mount):
        raise RuntimeError(f"required NAS is not mounted: {mount}")
    if not root.is_dir():
        raise RuntimeError(f"PROD artifact root is unavailable: {root}")
    if not (root / "readiness" / "latest.json").is_file():
        raise RuntimeError(f"PROD baseline receipt is missing under: {root}")
    return root


@contextmanager
def _exclusive_lock(path: Path = LOCK_PATH) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another SHARADAR_PROD daily update is running") from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _update_commands(service_date: str) -> list[tuple[str, list[str]]]:
    common = [
        "--deployment",
        "prod",
        "--as-of",
        service_date,
        "--confirmation",
        "SHARADAR_PROD_WRITE",
        "--production-confirmation",
        "BACKFILL_SHARADAR_PROD",
    ]
    python = str(REPO_ROOT / "venv" / "bin" / "python")
    return [
        (
            "lastupdated",
            [python, str(REPO_ROOT / "scripts" / "update_incremental.py"), *common],
        ),
        (
            "date_overlap",
            [python, str(REPO_ROOT / "scripts" / "update_date_overlap.py"), *common],
        ),
    ]


def main() -> int:
    args = parse_args()
    now = datetime.now(UTC)
    state = _read_state()
    should_run, reason, service_date = schedule_decision(
        now, state, force=args.force
    )
    if not should_run:
        _emit("SKIPPED", reason=reason, service_date=service_date)
        return 0

    try:
        artifact_root = _require_prod_nas()
    except RuntimeError as error:
        _emit("RETRY_REQUIRED", reason="prerequisite_failed", error=str(error))
        return 75

    if args.check_only:
        _emit(
            "READY",
            reason=reason,
            service_date=service_date,
            artifact_root=str(artifact_root),
        )
        return 0

    try:
        with _exclusive_lock():
            started_at = datetime.now(UTC)
            _emit("STARTED", reason=reason, service_date=service_date)
            completed_steps: list[str] = []
            for step, command in _update_commands(service_date):
                _emit("STEP_STARTED", step=step, service_date=service_date)
                result = subprocess.run(command, cwd=REPO_ROOT, check=False)
                if result.returncode != 0:
                    _emit(
                        "RETRY_REQUIRED",
                        reason="daily_update_step_failed",
                        step=step,
                        completed_steps=completed_steps,
                        service_date=service_date,
                        returncode=result.returncode,
                    )
                    return result.returncode or 1
                completed_steps.append(step)
            finished_at = datetime.now(UTC)
            _write_state(
                {
                    "version": 1,
                    "scheduler": LABEL,
                    "last_success_service_date": service_date,
                    "last_success_started_at": started_at.isoformat(),
                    "last_success_finished_at": finished_at.isoformat(),
                    "artifact_root": str(artifact_root),
                    "completed_steps": completed_steps,
                }
            )
            _emit(
                "COMPLETE",
                service_date=service_date,
                completed_steps=completed_steps,
            )
            return 0
    except RuntimeError as error:
        _emit("RETRY_REQUIRED", reason="lock_unavailable", error=str(error))
        return 75


if __name__ == "__main__":
    raise SystemExit(main())
