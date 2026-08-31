from __future__ import annotations

from datetime import UTC, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_prod_health.py"
SPEC = spec_from_file_location("check_prod_health", SCRIPT)
assert SPEC and SPEC.loader
MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_expected_daily_service_date_respects_eastern_cutoff() -> None:
    assert MODULE.expected_daily_service_date(
        datetime(2026, 9, 1, 4, 30, tzinfo=UTC)
    ) == "2026-08-31"
    assert MODULE.expected_daily_service_date(
        datetime(2026, 9, 1, 4, 45, tzinfo=UTC)
    ) == "2026-09-01"


def test_expected_monthly_service_month_rolls_after_window() -> None:
    assert MODULE.expected_monthly_service_month(
        datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    ) == "2026-08"
    assert MODULE.expected_monthly_service_month(
        datetime(2026, 9, 2, 7, 15, tzinfo=UTC)
    ) == "2026-09"
