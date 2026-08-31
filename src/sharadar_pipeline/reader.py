"""Bounded, read-only consumer interface for SHARADAR_DEV/PROD."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from typing import Any, Self

from .catalog import SharadarTable, normalize_table
from .runtime import MongoRuntime, connect_mongo_runtime
from .schema_registry import SchemaRegistry, load_schema_registry

AS_REPORTED_DIMENSIONS = frozenset({"ARQ", "ARY", "ART"})
MAX_TICKERS = 2_000
MAX_ROWS = 10_000


class SharadarReader:
    """Read query-ready vendor rows without granting write authority."""

    def __init__(
        self,
        runtime: MongoRuntime,
        *,
        registry: SchemaRegistry | None = None,
    ) -> None:
        self.runtime = runtime
        self.database = runtime.database
        self.registry = registry or load_schema_registry()

    @classmethod
    def connect(cls, deployment: str = "prod") -> Self:
        return cls(connect_mongo_runtime(deployment, write=False))

    def close(self) -> None:
        self.runtime.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def table_rows(
        self,
        table: SharadarTable | str,
        query: Mapping[str, Any] | None = None,
        *,
        fields: Sequence[str] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
        limit: int = 1_000,
    ) -> list[dict[str, Any]]:
        """Return a bounded raw-table slice with storage metadata excluded."""

        exact_table = normalize_table(table)
        schema = self.registry.table(exact_table)
        exact_limit = _bounded_limit(limit)
        projection = _projection(schema.ordered_headers, fields)
        cursor = self.database[exact_table.value].find(dict(query or {}), projection)
        if sort:
            cursor = cursor.sort(list(sort))
        rows = list(cursor.limit(exact_limit))
        return [_clean_dates(row, schema.date_columns) for row in rows]

    def as_reported_fundamentals(
        self,
        tickers: Sequence[str],
        as_of: date,
        *,
        dimension: str = "ARQ",
        fields: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return the latest report period known by ``as_of`` for each ticker.

        Only Sharadar's filing-date As-Reported lanes are accepted.  This method
        deliberately rejects MRQ/MRY/MRT so a consumer cannot silently turn a
        present-day restated value into a historical PIT observation.
        """

        exact_tickers = _ticker_list(tickers)
        if dimension not in AS_REPORTED_DIMENSIONS:
            raise ValueError("dimension must be one of ARQ, ARY, or ART")
        exact_as_of = _as_bson_date(as_of)
        schema = self.registry.table(SharadarTable.FUNDAMENTALS)
        projection = _projection(
            schema.ordered_headers,
            fields,
            required=("ticker", "dimension", "date", "reportperiod"),
        )
        pipeline = [
            {
                "$match": {
                    "ticker": {"$in": list(exact_tickers)},
                    "dimension": dimension,
                    "date": {"$lte": exact_as_of},
                }
            },
            {"$sort": {"ticker": 1, "reportperiod": -1, "date": -1}},
            {"$group": {"_id": "$ticker", "row": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$row"}},
            {"$project": projection},
            {"$sort": {"ticker": 1}},
        ]
        rows = list(self.database["fundamentals"].aggregate(pipeline))
        return [_clean_dates(row, schema.date_columns) for row in rows]

    def daily_metrics_as_of(
        self,
        tickers: Sequence[str],
        as_of: date,
        *,
        fields: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return each ticker's latest daily metrics row on or before ``as_of``."""

        exact_tickers = _ticker_list(tickers)
        exact_as_of = _as_bson_date(as_of)
        schema = self.registry.table(SharadarTable.DAILY)
        projection = _projection(
            schema.ordered_headers,
            fields,
            required=("ticker", "date", "lastupdated"),
        )
        pipeline = [
            {
                "$match": {
                    "ticker": {"$in": list(exact_tickers)},
                    "date": {"$lte": exact_as_of},
                }
            },
            {"$sort": {"ticker": 1, "date": -1}},
            {"$group": {"_id": "$ticker", "row": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$row"}},
            {"$project": projection},
            {"$sort": {"ticker": 1}},
        ]
        rows = list(self.database["daily"].aggregate(pipeline))
        return [_clean_dates(row, schema.date_columns) for row in rows]


def _bounded_limit(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= MAX_ROWS:
        raise ValueError(f"limit must be between 1 and {MAX_ROWS}")
    return value


def _ticker_list(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError("tickers must be an ordered sequence")
    result = tuple(values)
    if not result or len(result) > MAX_TICKERS:
        raise ValueError(f"tickers must contain between 1 and {MAX_TICKERS} values")
    if any(not isinstance(value, str) or not value or value != value.strip() for value in result):
        raise ValueError("tickers must contain non-empty trimmed strings")
    if len(set(result)) != len(result):
        raise ValueError("tickers cannot contain duplicates")
    return result


def _as_bson_date(value: date) -> datetime:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError("as_of must be a date")
    return datetime.combine(value, time.min, tzinfo=UTC)


def _projection(
    allowed: Sequence[str],
    fields: Sequence[str] | None,
    *,
    required: Sequence[str] = (),
) -> dict[str, int]:
    if fields is None:
        selected = tuple(allowed)
    else:
        if isinstance(fields, (str, bytes)) or not isinstance(fields, Sequence):
            raise TypeError("fields must be an ordered sequence")
        selected = tuple(fields)
        if not selected or len(set(selected)) != len(selected):
            raise ValueError("fields must be non-empty and unique")
        unknown = set(selected).difference(allowed)
        if unknown:
            raise ValueError(f"unknown fields: {sorted(unknown)!r}")
    ordered = tuple(dict.fromkeys((*required, *selected)))
    return {"_id": 0, **{field: 1 for field in ordered}}


def _clean_dates(
    row: Mapping[str, Any], date_columns: Sequence[str]
) -> dict[str, Any]:
    result = dict(row)
    result.pop("_id", None)
    for field in date_columns:
        value = result.get(field)
        if isinstance(value, datetime):
            result[field] = value.date()
    return result
