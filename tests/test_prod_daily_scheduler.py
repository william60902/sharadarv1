from __future__ import annotations

from datetime import UTC, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_prod_daily.py"
SPEC = spec_from_file_location("run_prod_daily", SCRIPT)
assert SPEC and SPEC.loader
MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_waits_until_eastern_cutoff() -> None:
    # 04:30 UTC is 00:30 EDT on 2026-08-31.
    decision = MODULE.schedule_decision(datetime(2026, 8, 31, 4, 30, tzinfo=UTC), {})
    assert decision == (False, "before_cutoff", "2026-08-31")


def test_due_once_per_eastern_service_date() -> None:
    now = datetime(2026, 8, 31, 4, 45, tzinfo=UTC)
    assert MODULE.schedule_decision(now, {}) == (True, "due", "2026-08-31")
    assert MODULE.schedule_decision(
        now, {"last_success_service_date": "2026-08-31"}
    ) == (False, "already_complete", "2026-08-31")


def test_force_does_not_change_service_date() -> None:
    now = datetime(2026, 8, 31, 3, 0, tzinfo=UTC)
    assert MODULE.schedule_decision(now, {}, force=True) == (
        True,
        "forced",
        "2026-08-30",
    )
