"""Immutable, versioned schema policy for Sharadar bulk admission.

The active registry is generated from Sharadar's public PostgreSQL schema
endpoints, reviewed, and committed as a package resource. Runtime ingestion
never refreshes schema policy from the network: an upstream change must first
be emitted as a candidate and promoted as a new registry version.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from functools import cache
from importlib import resources
from types import MappingProxyType
from typing import Final
from urllib.parse import urlsplit

from .catalog import TABLE_SPECS, Plan, SharadarTable, normalize_table

REGISTRY_NAME: Final = "fundamentals_plan"
REGISTRY_DIALECT: Final = "postgres"
ACTIVE_REGISTRY_VERSION: Final = 1
FUNDAMENTALS_TABLES: Final = tuple(
    table for table, spec in TABLE_SPECS.items() if Plan.FUNDAMENTALS in spec.plans
)
_RESOURCE_PACKAGE: Final = "sharadar_pipeline.resources"
_FIELD_PATTERN: Final = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_TYPE_PATTERN: Final = re.compile(r"^[a-z][a-z0-9 ]*$")
_SHA256_PATTERN: Final = re.compile(r"^[0-9a-f]{64}$")


class SchemaRegistryError(ValueError):
    """A packaged schema registry is missing, malformed, or inconsistent."""


@dataclass(frozen=True, slots=True)
class SchemaColumn:
    """One ordered PostgreSQL column from the official public schema."""

    name: str
    postgres_type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class TableSchema:
    """Exact admission schema and identity metadata for one Sharadar table."""

    table: SharadarTable
    columns: tuple[SchemaColumn, ...]
    ordered_headers: tuple[str, ...]
    primary_key: tuple[str, ...]
    date_columns: tuple[str, ...]
    source_url: str
    source_as_of: str
    source_sha256: str


@dataclass(frozen=True, slots=True)
class SchemaRegistry:
    """Validated immutable view of one committed registry resource."""

    version: int
    name: str
    dialect: str
    resource_sha256: str
    tables: MappingProxyType

    @property
    def expected_headers(self) -> MappingProxyType:
        """Exact header mapping accepted directly by ``BulkDownloader``."""

        return MappingProxyType(
            {table: schema.ordered_headers for table, schema in self.tables.items()}
        )

    @property
    def primary_keys(self) -> MappingProxyType:
        return MappingProxyType(
            {table: schema.primary_key for table, schema in self.tables.items()}
        )

    @property
    def date_columns(self) -> MappingProxyType:
        return MappingProxyType(
            {table: schema.date_columns for table, schema in self.tables.items()}
        )

    def table(self, value: SharadarTable | str) -> TableSchema:
        table = normalize_table(value)
        try:
            return self.tables[table]
        except KeyError:
            raise SchemaRegistryError(
                f"table {table.value!r} is outside the Fundamentals registry"
            ) from None


def _resource_name(version: int) -> str:
    if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
        raise SchemaRegistryError("schema registry version must be a positive integer")
    return f"{REGISTRY_NAME}.v{version}.json"


@cache
def load_schema_registry(version: int = ACTIVE_REGISTRY_VERSION) -> SchemaRegistry:
    """Load and strictly validate a committed registry without network I/O."""

    resource_name = _resource_name(version)
    try:
        raw = resources.files(_RESOURCE_PACKAGE).joinpath(resource_name).read_bytes()
    except (FileNotFoundError, ModuleNotFoundError):
        raise SchemaRegistryError(
            f"schema registry version {version} is not packaged"
        ) from None
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise SchemaRegistryError(
            "schema registry resource is not valid JSON"
        ) from None
    return _validate_registry_payload(
        payload,
        requested_version=version,
        resource_sha256=hashlib.sha256(raw).hexdigest(),
    )


def get_table_schema(
    table: SharadarTable | str,
    version: int = ACTIVE_REGISTRY_VERSION,
) -> TableSchema:
    return load_schema_registry(version).table(table)


def expected_headers(version: int = ACTIVE_REGISTRY_VERSION) -> MappingProxyType:
    """Return all exact Fundamentals-plan CSV headers for bulk admission."""

    return load_schema_registry(version).expected_headers


def _validate_registry_payload(
    payload: object,
    *,
    requested_version: int,
    resource_sha256: str,
) -> SchemaRegistry:
    if not isinstance(payload, dict):
        raise SchemaRegistryError("schema registry root must be an object")
    required_root = {
        "registry_version",
        "registry_name",
        "dialect",
        "tables",
    }
    if set(payload) != required_root:
        raise SchemaRegistryError("schema registry root fields do not match policy")
    if payload["registry_version"] != requested_version:
        raise SchemaRegistryError(
            "schema registry version does not match resource name"
        )
    if payload["registry_name"] != REGISTRY_NAME:
        raise SchemaRegistryError("schema registry name does not match policy")
    if payload["dialect"] != REGISTRY_DIALECT:
        raise SchemaRegistryError("schema registry dialect does not match policy")
    raw_tables = payload["tables"]
    if not isinstance(raw_tables, list):
        raise SchemaRegistryError("schema registry tables must be an ordered list")

    schemas: dict[SharadarTable, TableSchema] = {}
    for raw_table in raw_tables:
        schema = _validate_table_payload(raw_table)
        if schema.table in schemas:
            raise SchemaRegistryError("schema registry contains a duplicate table")
        schemas[schema.table] = schema
    if tuple(schemas) != FUNDAMENTALS_TABLES:
        raise SchemaRegistryError(
            "schema registry table order/set does not match the Fundamentals plan"
        )
    return SchemaRegistry(
        version=requested_version,
        name=REGISTRY_NAME,
        dialect=REGISTRY_DIALECT,
        resource_sha256=resource_sha256,
        tables=MappingProxyType(schemas),
    )


def _validate_table_payload(payload: object) -> TableSchema:
    if not isinstance(payload, dict):
        raise SchemaRegistryError("table schema must be an object")
    required = {
        "name",
        "source_url",
        "source_as_of",
        "source_sha256",
        "ordered_headers",
        "columns",
        "primary_key",
        "date_columns",
    }
    if set(payload) != required:
        raise SchemaRegistryError("table schema fields do not match policy")
    try:
        table = normalize_table(payload["name"])
    except (TypeError, ValueError):
        raise SchemaRegistryError("table schema has an unsupported name") from None
    if table not in FUNDAMENTALS_TABLES or payload["name"] != table.value:
        raise SchemaRegistryError(
            "table schema name is not canonical Fundamentals data"
        )

    columns_payload = payload["columns"]
    if not isinstance(columns_payload, list) or not columns_payload:
        raise SchemaRegistryError("table columns must be a non-empty ordered list")
    columns: list[SchemaColumn] = []
    for raw_column in columns_payload:
        if not isinstance(raw_column, dict) or set(raw_column) != {
            "name",
            "postgres_type",
            "nullable",
        }:
            raise SchemaRegistryError("column fields do not match policy")
        name = raw_column["name"]
        postgres_type = raw_column["postgres_type"]
        nullable = raw_column["nullable"]
        if not isinstance(name, str) or not _FIELD_PATTERN.fullmatch(name):
            raise SchemaRegistryError("column name is invalid")
        if not isinstance(postgres_type, str) or not _TYPE_PATTERN.fullmatch(
            postgres_type
        ):
            raise SchemaRegistryError("PostgreSQL column type is invalid")
        if type(nullable) is not bool:
            raise SchemaRegistryError("column nullable flag must be boolean")
        columns.append(SchemaColumn(name, postgres_type, nullable))
    column_names = tuple(column.name for column in columns)
    if len(column_names) != len(set(column_names)):
        raise SchemaRegistryError("table schema contains duplicate columns")

    ordered_headers = _ordered_field_tuple(
        payload["ordered_headers"], label="ordered headers", allow_empty=False
    )
    if ordered_headers != column_names:
        raise SchemaRegistryError("ordered headers do not exactly match column order")
    primary_key = _ordered_field_tuple(
        payload["primary_key"], label="primary key", allow_empty=False
    )
    if not set(primary_key).issubset(column_names):
        raise SchemaRegistryError("primary key references an unknown column")
    nullability = {column.name: column.nullable for column in columns}
    if any(nullability[field] for field in primary_key):
        raise SchemaRegistryError("primary key column cannot be nullable")
    date_columns = _ordered_field_tuple(
        payload["date_columns"], label="date columns", allow_empty=True
    )
    derived_dates = tuple(
        column.name for column in columns if column.postgres_type == "date"
    )
    if date_columns != derived_dates:
        raise SchemaRegistryError("date columns do not exactly match PostgreSQL types")

    source_url = payload["source_url"]
    expected_url = f"https://api.sharadar.com/v1.0/schema/{table.value}?format=postgres"
    if source_url != expected_url:
        raise SchemaRegistryError("schema source URL does not match official endpoint")
    parts = urlsplit(source_url)
    if parts.hostname != "api.sharadar.com" or parts.scheme != "https":
        raise SchemaRegistryError("schema source URL is not first-party HTTPS")
    source_as_of = payload["source_as_of"]
    if not isinstance(source_as_of, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}", source_as_of
    ):
        raise SchemaRegistryError("schema source date is invalid")
    try:
        date.fromisoformat(source_as_of)
    except ValueError:
        raise SchemaRegistryError("schema source date is invalid") from None
    source_sha256 = payload["source_sha256"]
    if not isinstance(source_sha256, str) or not _SHA256_PATTERN.fullmatch(
        source_sha256
    ):
        raise SchemaRegistryError("schema source digest is invalid")

    return TableSchema(
        table=table,
        columns=tuple(columns),
        ordered_headers=ordered_headers,
        primary_key=primary_key,
        date_columns=date_columns,
        source_url=source_url,
        source_as_of=source_as_of,
        source_sha256=source_sha256,
    )


def _ordered_field_tuple(
    value: object, *, label: str, allow_empty: bool
) -> tuple[str, ...]:
    if not isinstance(value, list) or (not allow_empty and not value):
        raise SchemaRegistryError(f"{label} must be an ordered list")
    if any(
        not isinstance(item, str) or not _FIELD_PATTERN.fullmatch(item)
        for item in value
    ):
        raise SchemaRegistryError(f"{label} contains an invalid field")
    if len(value) != len(set(value)):
        raise SchemaRegistryError(f"{label} contains duplicate fields")
    return tuple(value)
