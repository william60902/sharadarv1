from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from sharadar_pipeline.bulk_pipeline import iter_bulk_csv_rows


def _archive(path: Path, body: str) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr("daily.csv", body)
    return path


def test_bulk_csv_iterator_is_ordered_and_streaming(tmp_path: Path) -> None:
    path = _archive(
        tmp_path / "daily.zip",
        "ticker,date,value\nAAPL,2026-08-28,1.5\nMSFT,2026-08-28,2.5\n",
    )
    rows = iter_bulk_csv_rows(
        path,
        member="daily.csv",
        expected_headers=("ticker", "date", "value"),
    )
    assert iter(rows) is rows
    assert list(rows) == [
        {"ticker": "AAPL", "date": "2026-08-28", "value": "1.5"},
        {"ticker": "MSFT", "date": "2026-08-28", "value": "2.5"},
    ]


def test_bulk_csv_iterator_rejects_header_drift(tmp_path: Path) -> None:
    path = _archive(tmp_path / "daily.zip", "date,ticker\n2026-08-28,AAPL\n")
    with pytest.raises(RuntimeError, match="headers"):
        list(
            iter_bulk_csv_rows(
                path,
                member="daily.csv",
                expected_headers=("ticker", "date"),
            )
        )
