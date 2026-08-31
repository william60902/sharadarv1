from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from sharadar_pipeline.reader import SharadarReader
from sharadar_pipeline.runtime import MongoRuntime
from sharadar_pipeline.routes import route_for


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def sort(self, _sort):
        return self

    def limit(self, value):
        return iter(self.rows[:value])


class _Collection:
    def __init__(self, rows):
        self.rows = rows
        self.pipeline = None

    def find(self, _query, _projection):
        return _Cursor(self.rows)

    def aggregate(self, pipeline):
        self.pipeline = pipeline
        return iter(self.rows)


class _Database:
    def __init__(self):
        self.collections = {
            "fundamentals": _Collection(
                [
                    {
                        "ticker": "AAPL",
                        "dimension": "ARQ",
                        "date": datetime(2026, 8, 1, tzinfo=UTC),
                        "reportperiod": datetime(2026, 6, 30, tzinfo=UTC),
                        "revenue": 1.0,
                    }
                ]
            ),
            "daily": _Collection([]),
        }

    def __getitem__(self, name):
        return self.collections[name]


class _Client:
    def close(self):
        pass


def _reader() -> tuple[SharadarReader, _Database]:
    database = _Database()
    runtime = MongoRuntime(route_for("prod"), _Client(), database)
    return SharadarReader(runtime), database


def test_as_reported_reader_enforces_pit_lane_and_as_of_boundary() -> None:
    reader, database = _reader()
    rows = reader.as_reported_fundamentals(
        ["AAPL"], date(2026, 8, 2), fields=("revenue",)
    )

    assert rows[0]["date"] == date(2026, 8, 1)
    pipeline = database["fundamentals"].pipeline
    assert pipeline[0]["$match"] == {
        "ticker": {"$in": ["AAPL"]},
        "dimension": "ARQ",
        "date": {"$lte": datetime(2026, 8, 2, tzinfo=UTC)},
    }
    assert pipeline[4]["$project"] == {
        "_id": 0,
        "ticker": 1,
        "dimension": 1,
        "date": 1,
        "reportperiod": 1,
        "revenue": 1,
    }

    with pytest.raises(ValueError, match="ARQ"):
        reader.as_reported_fundamentals(["AAPL"], date(2026, 8, 2), dimension="MRQ")


def test_reader_rejects_unbounded_or_duplicate_consumer_inputs() -> None:
    reader, _database = _reader()
    with pytest.raises(ValueError, match="duplicates"):
        reader.daily_metrics_as_of(["AAPL", "AAPL"], date(2026, 8, 2))
    with pytest.raises(ValueError, match="limit"):
        reader.table_rows("fundamentals", limit=10_001)
