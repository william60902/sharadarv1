from __future__ import annotations

from datetime import UTC, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_prod_monthly.py"
SPEC = spec_from_file_location("run_prod_monthly", SCRIPT)
assert SPEC and SPEC.loader
MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_monthly_waits_until_second_day_window() -> None:
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)  # 03:00 EDT
    assert MODULE.monthly_decision(now, {}) == (
        False,
        "before_monthly_window",
        "2026-09",
    )


def test_monthly_runs_once_and_catches_up_after_missed_day() -> None:
    due = datetime(2026, 9, 2, 7, 15, tzinfo=UTC)
    assert MODULE.monthly_decision(due, {}) == (True, "due", "2026-09")
    assert MODULE.monthly_decision(
        due, {"last_success_month": "2026-09"}
    ) == (False, "already_complete", "2026-09")
    later = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    assert MODULE.monthly_decision(later, {}) == (True, "due", "2026-09")
