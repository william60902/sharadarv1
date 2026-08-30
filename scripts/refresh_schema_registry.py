#!/usr/bin/env python3
"""Fetch official public PostgreSQL schemas and emit a review candidate.

This script never uses an API key and never modifies the active packaged
registry. Review the candidate diff and promote it as a new version explicitly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.schema_registry import (
    ACTIVE_REGISTRY_VERSION,
    FUNDAMENTALS_TABLES,
    REGISTRY_DIALECT,
    REGISTRY_NAME,
)

API_BASE = "https://api.sharadar.com/v1.0"
MAX_SCHEMA_BYTES = 1024 * 1024
_AS_OF_PATTERN = re.compile(r"^-- As of (\d{4}-\d{2}-\d{2})$", re.MULTILINE)
_IDENTIFIER = (
    r'(?:"(?P<quoted>[A-Za-z][A-Za-z0-9_]*)"|(?P<plain>[A-Za-z][A-Za-z0-9_]*))'
)
_COLUMN_PATTERN = re.compile(
    rf"^\s*{_IDENTIFIER}\s+(?P<type>[a-z][a-z0-9 ]*?)"
    r"(?P<not_null>\s+NOT NULL)?\s*,?\s*$"
)
_PRIMARY_KEY_PATTERN = re.compile(r"^\s*PRIMARY KEY\s*\((?P<fields>[^)]+)\)\s*,?\s*$")
_FIELD_PATTERN = re.compile(
    r'^\s*(?:"(?P<quoted>[A-Za-z][A-Za-z0-9_]*)"|(?P<plain>[A-Za-z][A-Za-z0-9_]*))\s*$'
)


def parse_postgres_schema(table: str, raw: bytes) -> dict[str, object]:
    """Parse the constrained official CREATE TABLE response exactly."""

    if not raw or len(raw) > MAX_SCHEMA_BYTES:
        raise ValueError("schema response size is empty or exceeds policy")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("schema response is not UTF-8") from None
    if "\x00" in text:
        raise ValueError("schema response contains NUL")
    title = f"-- Sharadar table schema: {table} (PostgreSQL)"
    if not text.startswith(title + "\n"):
        raise ValueError("schema response title does not match requested table")
    as_of_match = _AS_OF_PATTERN.search(text)
    if as_of_match is None:
        raise ValueError("schema response is missing its as-of date")
    create_prefix = f"CREATE TABLE IF NOT EXISTS {table} (\n"
    start = text.find(create_prefix)
    if start < 0:
        raise ValueError("schema response is missing the expected CREATE TABLE")
    body_start = start + len(create_prefix)
    body_end = text.find("\n);", body_start)
    if body_end < 0 or text.find(create_prefix, body_start) >= 0:
        raise ValueError("schema response has an ambiguous CREATE TABLE body")

    columns: list[dict[str, object]] = []
    primary_key: list[str] | None = None
    for line in text[body_start:body_end].splitlines():
        pk_match = _PRIMARY_KEY_PATTERN.fullmatch(line)
        if pk_match is not None:
            if primary_key is not None:
                raise ValueError("schema response contains multiple primary keys")
            primary_key = []
            for raw_field in pk_match.group("fields").split(","):
                field_match = _FIELD_PATTERN.fullmatch(raw_field)
                if field_match is None:
                    raise ValueError("schema primary key contains an invalid field")
                primary_key.append(
                    field_match.group("quoted") or field_match.group("plain")
                )
            continue
        column_match = _COLUMN_PATTERN.fullmatch(line)
        if column_match is None:
            raise ValueError("schema CREATE TABLE contains an unsupported line")
        name = column_match.group("quoted") or column_match.group("plain")
        columns.append(
            {
                "name": name,
                "postgres_type": column_match.group("type").strip(),
                "nullable": column_match.group("not_null") is None,
            }
        )
    if not columns or not primary_key:
        raise ValueError("schema must contain columns and one primary key")
    headers = [column["name"] for column in columns]
    if len(headers) != len(set(headers)):
        raise ValueError("schema contains duplicate columns")
    if len(primary_key) != len(set(primary_key)) or not set(primary_key).issubset(
        headers
    ):
        raise ValueError(
            "schema primary key is duplicated or references unknown columns"
        )
    nullable = {column["name"]: column["nullable"] for column in columns}
    if any(nullable[field] for field in primary_key):
        raise ValueError("schema primary key field is nullable")

    return {
        "name": table,
        "source_url": f"{API_BASE}/schema/{table}?format=postgres",
        "source_as_of": as_of_match.group(1),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "ordered_headers": headers,
        "columns": columns,
        "primary_key": primary_key,
        "date_columns": [
            column["name"] for column in columns if column["postgres_type"] == "date"
        ],
    }


def fetch_candidate(*, timeout: float = 30.0) -> dict[str, object]:
    session = requests.Session()
    session.trust_env = False
    tables: list[dict[str, object]] = []
    try:
        for table in FUNDAMENTALS_TABLES:
            response = session.get(
                f"{API_BASE}/schema/{table.value}",
                params={"format": REGISTRY_DIALECT},
                headers={"Accept": "text/plain"},
                timeout=(10.0, timeout),
                allow_redirects=False,
            )
            if response.status_code != 200:
                raise RuntimeError(
                    f"schema endpoint for {table.value} returned {response.status_code}"
                )
            content_type = response.headers.get("Content-Type", "").lower()
            if not content_type.startswith("text/plain"):
                raise RuntimeError(
                    f"schema endpoint for {table.value} returned unexpected content type"
                )
            tables.append(parse_postgres_schema(table.value, response.content))
    finally:
        session.close()
    return {
        "registry_version": ACTIVE_REGISTRY_VERSION,
        "registry_name": REGISTRY_NAME,
        "dialect": REGISTRY_DIALECT,
        "tables": tables,
    }


def encode_candidate(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, indent=2, ensure_ascii=True) + "\n").encode("utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit a Sharadar Fundamentals schema-registry review candidate."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Candidate output path; omit to print JSON to stdout.",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.timeout <= 0:
        raise SystemExit("--timeout must be positive")
    encoded = encode_candidate(fetch_candidate(timeout=args.timeout))
    if args.output is None:
        sys.stdout.buffer.write(encoded)
        return 0
    active = (
        SRC_ROOT
        / "sharadar_pipeline"
        / "resources"
        / f"{REGISTRY_NAME}.v{ACTIVE_REGISTRY_VERSION}.json"
    ).resolve()
    output = args.output.expanduser().resolve()
    if output == active:
        raise SystemExit("refusing to overwrite the active packaged registry")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(encoded)
    print(f"candidate={output} sha256={hashlib.sha256(encoded).hexdigest()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
