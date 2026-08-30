from __future__ import annotations

import hashlib
import json
from importlib import resources

import pytest

from scripts.refresh_schema_registry import encode_candidate, parse_postgres_schema
from sharadar_pipeline.catalog import SharadarTable
from sharadar_pipeline.schema_registry import (
    FUNDAMENTALS_TABLES,
    SchemaRegistryError,
    _validate_registry_payload,
    expected_headers,
    get_table_schema,
    load_schema_registry,
)


def test_packaged_registry_is_exact_ordered_and_bulk_ready() -> None:
    registry = load_schema_registry()

    assert tuple(registry.tables) == FUNDAMENTALS_TABLES
    assert tuple(registry.expected_headers) == FUNDAMENTALS_TABLES
    assert expected_headers() == registry.expected_headers
    assert (
        registry.resource_sha256
        == hashlib.sha256(
            resources.files("sharadar_pipeline.resources")
            .joinpath("fundamentals_plan.v1.json")
            .read_bytes()
        ).hexdigest()
    )
    for table, schema in registry.tables.items():
        assert schema.table is table
        assert schema.ordered_headers == tuple(column.name for column in schema.columns)
        assert schema.primary_key
        assert set(schema.primary_key).issubset(schema.ordered_headers)
        assert schema.date_columns == tuple(
            column.name for column in schema.columns if column.postgres_type == "date"
        )
        assert len(schema.source_sha256) == 64


def test_known_official_identity_fields_and_alias_lookup() -> None:
    fundamentals = get_table_schema("SF1")
    assert fundamentals.table is SharadarTable.FUNDAMENTALS
    assert fundamentals.ordered_headers[:7] == (
        "ticker",
        "dimension",
        "calendardate",
        "date",
        "reportperiod",
        "fiscalperiod",
        "lastupdated",
    )
    assert fundamentals.primary_key == (
        "ticker",
        "dimension",
        "date",
        "reportperiod",
    )
    assert fundamentals.date_columns == (
        "calendardate",
        "date",
        "reportperiod",
        "lastupdated",
    )
    assert get_table_schema("sp500").primary_key == ("date", "action", "ticker")


def test_non_fundamentals_table_is_rejected() -> None:
    with pytest.raises(SchemaRegistryError, match="outside"):
        get_table_schema("stocks")


def test_registry_rejects_header_drift() -> None:
    raw = (
        resources.files("sharadar_pipeline.resources")
        .joinpath("fundamentals_plan.v1.json")
        .read_bytes()
    )
    payload = json.loads(raw)
    payload["tables"][0]["ordered_headers"].append("unexpected")
    with pytest.raises(SchemaRegistryError, match="headers"):
        _validate_registry_payload(
            payload,
            requested_version=1,
            resource_sha256="0" * 64,
        )


def test_public_postgres_parser_preserves_column_pk_and_date_order() -> None:
    raw = b"""-- Sharadar table schema: sample (PostgreSQL)\n-- As of 2026-08-18\n-- https://api.sharadar.com/v1.0/schema/sample?format=postgres\n\nCREATE TABLE IF NOT EXISTS sample (\n  \"table\" text NOT NULL,\n  ticker text NOT NULL,\n  date date NOT NULL,\n  value double precision,\n  PRIMARY KEY (\"table\", ticker, date)\n);\n"""
    parsed = parse_postgres_schema("sample", raw)
    assert parsed["ordered_headers"] == ["table", "ticker", "date", "value"]
    assert parsed["primary_key"] == ["table", "ticker", "date"]
    assert parsed["date_columns"] == ["date"]
    assert parsed["columns"][-1] == {
        "name": "value",
        "postgres_type": "double precision",
        "nullable": True,
    }
    assert parsed["source_sha256"] == hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"-- wrong\n",
        b"-- Sharadar table schema: sample (PostgreSQL)\n-- As of 2026-08-18\n",
    ],
)
def test_public_postgres_parser_rejects_incomplete_or_wrong_input(raw: bytes) -> None:
    with pytest.raises(ValueError):
        parse_postgres_schema("sample", raw)


def test_candidate_encoding_is_deterministic() -> None:
    payload = {
        "registry_version": 1,
        "registry_name": "fundamentals_plan",
        "dialect": "postgres",
        "tables": [],
    }
    assert encode_candidate(payload) == encode_candidate(payload)
    assert encode_candidate(payload).endswith(b"\n")
