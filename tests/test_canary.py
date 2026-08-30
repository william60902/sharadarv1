from __future__ import annotations

from datetime import date

import pytest

from sharadar_pipeline.canary import (
    DEFAULT_CANARY_SYMBOLS,
    canary_query_specs,
    fetch_canary_batches,
)
from sharadar_pipeline.catalog import SharadarTable


class FakeClient:
    def __init__(self, *, only_one_arq: bool = False) -> None:
        self.only_one_arq = only_one_arq

    def query_spec(self, spec):
        if spec.table == "fundamentals":
            periods = ("2026-03-31",) if self.only_one_arq else (
                "2026-03-31",
                "2026-06-30",
            )
            return {
                "data": [
                    {
                        "ticker": symbol,
                        "dimension": "ARQ",
                        "date": "2026-05-01",
                        "reportperiod": period,
                    }
                    for symbol in DEFAULT_CANARY_SYMBOLS
                    for period in periods
                ]
            }
        return {"data": [{"date": "2026-08-29", "table": str(spec.table)}]}


def test_canary_plan_is_bounded_and_covers_paid_tables_once() -> None:
    specs = canary_query_specs(as_of=date(2026, 8, 31))
    assert tuple(SharadarTable(spec.table) for spec in specs) == (
        SharadarTable.DESCRIPTIONS,
        SharadarTable.TICKERS,
        SharadarTable.FUNDAMENTALS,
        SharadarTable.DAILY,
        SharadarTable.ACTIONS,
        SharadarTable.EVENTS,
        SharadarTable.SP500,
    )
    assert all(spec.limit is not None and spec.limit <= 2_000 for spec in specs)


def test_fetch_canary_requires_two_arq_periods_for_every_symbol() -> None:
    batches = fetch_canary_batches(FakeClient(), as_of=date(2026, 8, 31))
    assert len(batches) == 7
    with pytest.raises(ValueError, match="two ARQ"):
        fetch_canary_batches(
            FakeClient(only_one_arq=True), as_of=date(2026, 8, 31)
        )
