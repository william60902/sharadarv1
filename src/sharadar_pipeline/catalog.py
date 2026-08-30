"""Code-owned catalog of Sharadar tables, aliases, plans and formats."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class Plan(StrEnum):
    FUNDAMENTALS = "fundamentals"
    PRICES = "prices"
    INVESTORS = "investors"
    BUNDLE = "bundle"


class SharadarTable(StrEnum):
    DESCRIPTIONS = "descriptions"
    TICKERS = "tickers"
    FUNDAMENTALS = "fundamentals"
    DAILY = "daily"
    ACTIONS = "actions"
    EVENTS = "events"
    SP500 = "sp500"
    STOCKS = "stocks"
    FUNDS = "funds"
    METRICS = "metrics"
    INSIDERS = "insiders"
    HOLDINGS = "holdings"
    HOLDINGS_TICKER = "holdings_ticker"
    HOLDINGS_INVESTOR = "holdings_investor"


class HistoryWindow(StrEnum):
    FIVE_YEARS = "5"
    TEN_YEARS = "10"
    FULL = "full"


class OutputFormat(StrEnum):
    CSV = "csv"
    JSON = "json"


class SchemaDialect(StrEnum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    MYSQL = "mysql"


@dataclass(frozen=True, slots=True)
class TableSpec:
    table: SharadarTable
    legacy_aliases: tuple[str, ...]
    plans: frozenset[Plan]
    supports_lastupdated: bool
    snapshot_only: bool = False


FUNDAMENTALS_PLAN: Final = frozenset({Plan.FUNDAMENTALS, Plan.BUNDLE})
PRICES_PLAN: Final = frozenset({Plan.PRICES, Plan.BUNDLE})
INVESTORS_PLAN: Final = frozenset({Plan.INVESTORS, Plan.BUNDLE})
ALL_PLANS: Final = frozenset(Plan)


_TABLE_SPECS = {
    SharadarTable.DESCRIPTIONS: TableSpec(
        SharadarTable.DESCRIPTIONS,
        ("indicator", "indicators"),
        ALL_PLANS,
        False,
        True,
    ),
    SharadarTable.TICKERS: TableSpec(
        SharadarTable.TICKERS, (), ALL_PLANS, True, True
    ),
    SharadarTable.FUNDAMENTALS: TableSpec(
        SharadarTable.FUNDAMENTALS, ("SF1",), FUNDAMENTALS_PLAN, True
    ),
    SharadarTable.DAILY: TableSpec(
        SharadarTable.DAILY, (), FUNDAMENTALS_PLAN, True
    ),
    SharadarTable.ACTIONS: TableSpec(
        SharadarTable.ACTIONS,
        (),
        frozenset({Plan.FUNDAMENTALS, Plan.PRICES, Plan.BUNDLE}),
        False,
    ),
    SharadarTable.EVENTS: TableSpec(
        SharadarTable.EVENTS, (), FUNDAMENTALS_PLAN, False
    ),
    SharadarTable.SP500: TableSpec(
        SharadarTable.SP500,
        (),
        frozenset({Plan.FUNDAMENTALS, Plan.PRICES, Plan.BUNDLE}),
        False,
    ),
    SharadarTable.STOCKS: TableSpec(
        SharadarTable.STOCKS, ("SEP",), PRICES_PLAN, True
    ),
    SharadarTable.FUNDS: TableSpec(
        SharadarTable.FUNDS, ("SFP",), PRICES_PLAN, True
    ),
    SharadarTable.METRICS: TableSpec(
        SharadarTable.METRICS, (), PRICES_PLAN, True
    ),
    SharadarTable.INSIDERS: TableSpec(
        SharadarTable.INSIDERS, ("SF2",), INVESTORS_PLAN, True
    ),
    SharadarTable.HOLDINGS: TableSpec(
        SharadarTable.HOLDINGS, ("SF3",), INVESTORS_PLAN, True
    ),
    SharadarTable.HOLDINGS_TICKER: TableSpec(
        SharadarTable.HOLDINGS_TICKER, ("SF3A",), INVESTORS_PLAN, True
    ),
    SharadarTable.HOLDINGS_INVESTOR: TableSpec(
        SharadarTable.HOLDINGS_INVESTOR, ("SF3B",), INVESTORS_PLAN, True
    ),
}

TABLE_SPECS = MappingProxyType(_TABLE_SPECS)


def _alias_key(value: str) -> str:
    return value.strip().replace("-", "_").lower()


_ALIASES: dict[str, SharadarTable] = {
    _alias_key(table.value): table for table in SharadarTable
}
for _spec in _TABLE_SPECS.values():
    for _alias in _spec.legacy_aliases:
        _ALIASES[_alias_key(_alias)] = _spec.table

TABLE_ALIASES = MappingProxyType(_ALIASES)


def normalize_table(value: SharadarTable | str) -> SharadarTable:
    if isinstance(value, SharadarTable):
        return value
    if type(value) is not str or not value.strip():
        raise ValueError("Sharadar table must be a non-empty string or SharadarTable")
    try:
        return TABLE_ALIASES[_alias_key(value)]
    except KeyError:
        raise ValueError(f"unsupported Sharadar table: {value!r}") from None


def table_spec(value: SharadarTable | str) -> TableSpec:
    return TABLE_SPECS[normalize_table(value)]
