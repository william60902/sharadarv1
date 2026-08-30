"""Bounded real-data query plan used before Sharadar production promotion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from .catalog import SharadarTable
from .client import (
    FilterOperator,
    QueryFilter,
    QuerySpec,
    SortDirection,
    SortSpec,
)

DEFAULT_CANARY_SYMBOLS = ("AAPL", "MSFT", "NVDA", "JPM", "XOM", "UNH")


@dataclass(frozen=True, slots=True)
class CanaryTableBatch:
    table: SharadarTable
    query: QuerySpec
    rows: tuple[Mapping[str, Any], ...]
    source_watermark: str


def canary_query_specs(
    *,
    as_of: date,
    symbols: Sequence[str] = DEFAULT_CANARY_SYMBOLS,
) -> tuple[QuerySpec, ...]:
    """Return the seven bounded, deterministic Fundamentals-plan queries."""

    tickers = tuple(symbols)
    return (
        QuerySpec(SharadarTable.DESCRIPTIONS, limit=250),
        QuerySpec(SharadarTable.TICKERS, tickers=tickers, limit=100),
        QuerySpec(
            SharadarTable.FUNDAMENTALS,
            tickers=tickers,
            filters=(
                QueryFilter(
                    "dimension",
                    FilterOperator.EQ,
                    ("ARQ", "ARY", "ART"),
                ),
            ),
            sort=SortSpec("date", SortDirection.DESC),
            limit=1_000,
        ),
        QuerySpec(
            SharadarTable.DAILY,
            tickers=tickers,
            from_date=as_of - timedelta(days=180),
            to_date=as_of,
            sort=SortSpec("date", SortDirection.ASC),
            limit=2_000,
        ),
        QuerySpec(
            SharadarTable.ACTIONS,
            tickers=tickers,
            sort=SortSpec("date", SortDirection.DESC),
            limit=250,
        ),
        QuerySpec(
            SharadarTable.EVENTS,
            tickers=tickers,
            sort=SortSpec("date", SortDirection.DESC),
            limit=250,
        ),
        QuerySpec(
            SharadarTable.SP500,
            sort=SortSpec("date", SortDirection.DESC),
            limit=250,
        ),
    )


def fetch_canary_batches(
    client: Any,
    *,
    as_of: date,
    symbols: Sequence[str] = DEFAULT_CANARY_SYMBOLS,
) -> tuple[CanaryTableBatch, ...]:
    """Fetch and minimally validate all seven small DEV slices."""

    batches: list[CanaryTableBatch] = []
    for spec in canary_query_specs(as_of=as_of, symbols=symbols):
        payload = client.query_spec(spec)
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), list):
            raise TypeError(f"{spec.table}: unexpected Sharadar JSON envelope")
        raw_rows = payload["data"]
        if not raw_rows or any(not isinstance(row, Mapping) for row in raw_rows):
            raise ValueError(f"{spec.table}: DEV canary returned no usable rows")
        rows = tuple(raw_rows)
        watermark = _source_watermark(rows, fallback=as_of.isoformat())
        batches.append(
            CanaryTableBatch(
                table=SharadarTable(spec.table),
                query=spec,
                rows=rows,
                source_watermark=watermark,
            )
        )
    _require_two_arq_quarters(batches, symbols=tuple(symbols))
    return tuple(batches)


def _source_watermark(rows: Sequence[Mapping[str, Any]], *, fallback: str) -> str:
    for field in ("lastupdated", "date", "reportperiod"):
        values = [str(row[field]) for row in rows if row.get(field)]
        if values:
            return max(values)
    return fallback


def _require_two_arq_quarters(
    batches: Sequence[CanaryTableBatch], *, symbols: tuple[str, ...]
) -> None:
    fundamental = next(
        (batch for batch in batches if batch.table is SharadarTable.FUNDAMENTALS),
        None,
    )
    if fundamental is None:
        raise ValueError("fundamentals DEV canary is missing")
    periods: dict[str, set[str]] = {symbol: set() for symbol in symbols}
    for row in fundamental.rows:
        ticker = row.get("ticker")
        period = row.get("reportperiod")
        if row.get("dimension") == "ARQ" and ticker in periods and period:
            periods[str(ticker)].add(str(period))
    failures = sorted(symbol for symbol, values in periods.items() if len(values) < 2)
    if failures:
        raise ValueError(
            "fundamentals DEV canary needs two ARQ report periods for: "
            + ",".join(failures)
        )
