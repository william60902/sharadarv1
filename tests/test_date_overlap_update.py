from __future__ import annotations

from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from sharadar_pipeline.catalog import SharadarTable


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "update_date_overlap.py"
SPEC = spec_from_file_location("update_date_overlap", SCRIPT)
assert SPEC and SPEC.loader
MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_events_window_stops_at_service_date() -> None:
    assert MODULE.date_window(
        SharadarTable.EVENTS,
        date(2026, 8, 31),
        lookback_days=35,
        actions_forward_days=370,
    ) == (date(2026, 7, 27), date(2026, 8, 31))


def test_actions_window_includes_announced_future_actions() -> None:
    assert MODULE.date_window(
        SharadarTable.ACTIONS,
        date(2026, 8, 31),
        lookback_days=35,
        actions_forward_days=370,
    ) == (date(2026, 7, 27), date(2027, 9, 5))
