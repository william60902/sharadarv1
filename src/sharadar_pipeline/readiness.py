"""Read-only SHARADAR_DEV to SHARADAR_PROD promotion readiness checks.

The validator deliberately has no write method and no production route.  Storage
implementations expose a small evidence-source protocol so this gate can inspect
MongoDB, Parquet manifests, or an in-memory canary without coupling validation to
the ingestion implementation.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from .catalog import Plan, SharadarTable, normalize_table, table_spec
from .routes import Deployment, SharadarRoute, route_for

READINESS_FORMAT = "sharadar.dev-readiness/v1"

_PIT_CLOCK_POLICY: Mapping[
    SharadarTable, tuple[tuple[str, ...], tuple[tuple[str, str], ...]]
] = {
    SharadarTable.DESCRIPTIONS: ((), ()),
    SharadarTable.TICKERS: (("lastupdated",), ()),
    SharadarTable.FUNDAMENTALS: (
        ("reportperiod", "date", "calendardate", "lastupdated"),
        (("reportperiod", "date"), ("date", "lastupdated")),
    ),
    SharadarTable.DAILY: (
        ("date", "lastupdated"),
        (("date", "lastupdated"),),
    ),
    SharadarTable.ACTIONS: (("date",), ()),
    SharadarTable.EVENTS: (("date",), ()),
    SharadarTable.SP500: (("date",), ()),
}


class ReadinessConfigurationError(ValueError):
    """The gate itself is not configured safely enough to run."""


@dataclass(frozen=True, slots=True)
class TableGateSpec:
    """Pinned expectations used by the read-only promotion gate."""

    table: SharadarTable
    primary_key: tuple[str, ...]
    schema_digest: str
    pit_clock_fields: tuple[str, ...] = ()
    pit_clock_order: tuple[tuple[str, str], ...] = ()
    required_indexes: tuple[tuple[str, ...], ...] = ()

    def __post_init__(self) -> None:
        if not self.primary_key or any(not value for value in self.primary_key):
            raise ReadinessConfigurationError(
                f"{self.table.value}: primary key must be pinned"
            )
        if not _is_sha256(self.schema_digest):
            raise ReadinessConfigurationError(
                f"{self.table.value}: schema digest must be a SHA-256 hex digest"
            )
        clock_set = set(self.pit_clock_fields)
        for earlier, later in self.pit_clock_order:
            if earlier not in clock_set or later not in clock_set:
                raise ReadinessConfigurationError(
                    f"{self.table.value}: clock order references an unpinned field"
                )


@dataclass(frozen=True, slots=True)
class TableEvidence:
    """Bounded evidence collected from one published DEV table."""

    manifest_id: str | None
    manifest_published: bool
    manifest_row_count: int | None
    stored_row_count: int
    primary_key_null_rows: int
    duplicate_primary_keys: int
    actual_schema_digest: str | None
    pit_clock_missing_rows: int
    pit_clock_order_violations: int
    artifact_checksum_verified: bool
    replay_verified: bool
    required_indexes_present: bool
    watermark: str | None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FundamentalsCanaryEvidence:
    """ARQ issuers with at least two distinct report periods."""

    qualifying_issuer_count: int
    sample_issuers: tuple[str, ...] = ()


class ReadinessEvidenceSource(Protocol):
    """Duck-typed, read-only evidence source used by :func:`verify_dev_readiness`."""

    def inspect_table(self, spec: TableGateSpec) -> TableEvidence: ...

    def inspect_fundamentals_arq_two_quarter(
        self,
    ) -> FundamentalsCanaryEvidence: ...


@dataclass(frozen=True, slots=True)
class GateCheck:
    name: str
    passed: bool
    table: str | None = None
    expected: Any = None
    actual: Any = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    format: str
    checked_at: str
    deployment: str
    database_name: str
    tables: tuple[str, ...]
    ready_for_prod_backfill: bool
    prod_write_authorized: bool
    failed_checks: int
    checks: tuple[GateCheck, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(
            self.to_dict(), sort_keys=True, indent=indent, ensure_ascii=False
        )


def verify_dev_readiness(
    source: ReadinessEvidenceSource,
    specs: Sequence[TableGateSpec],
    *,
    route: SharadarRoute | None = None,
    minimum_arq_two_quarter_issuers: int = 1,
    checked_at: datetime | None = None,
) -> ReadinessReport:
    """Return a deterministic, machine-readable, fail-closed DEV gate report.

    This function accepts only the canonical DEV route and never performs writes.
    A ``ready`` result is evidence that a PROD backfill may be *prepared*; it is
    intentionally not authorization to execute that backfill.
    """

    canonical_dev = route_for(Deployment.DEV)
    selected_route = canonical_dev if route is None else route
    if selected_route is not canonical_dev:
        raise ReadinessConfigurationError(
            "readiness verification accepts only the canonical SHARADAR_DEV route"
        )
    if type(minimum_arq_two_quarter_issuers) is not int:
        raise ReadinessConfigurationError("canary minimum must be an integer")
    if minimum_arq_two_quarter_issuers < 1:
        raise ReadinessConfigurationError("canary minimum must be positive")

    expected_tables = tuple(spec.table for spec in specs)
    required_tables = tuple(
        table
        for table in SharadarTable
        if Plan.FUNDAMENTALS in table_spec(table).plans
    )
    if expected_tables != required_tables or len(set(expected_tables)) != len(
        expected_tables
    ):
        raise ReadinessConfigurationError(
            "specs must contain the seven Fundamentals-plan tables exactly once "
            "in catalog order"
        )

    checks: list[GateCheck] = []
    for spec in specs:
        try:
            evidence = source.inspect_table(spec)
        except Exception as exc:  # noqa: BLE001 - adapter boundary fails closed
            checks.append(
                GateCheck(
                    name="evidence_collected",
                    table=spec.table.value,
                    passed=False,
                    expected="readable bounded evidence",
                    actual=type(exc).__name__,
                    detail="evidence source failed closed",
                )
            )
            continue
        checks.extend(_table_checks(spec, evidence))

    try:
        canary = source.inspect_fundamentals_arq_two_quarter()
        checks.append(
            GateCheck(
                name="fundamentals_arq_two_quarter_canary",
                table=SharadarTable.FUNDAMENTALS.value,
                passed=(
                    canary.qualifying_issuer_count
                    >= minimum_arq_two_quarter_issuers
                ),
                expected=f">={minimum_arq_two_quarter_issuers}",
                actual=canary.qualifying_issuer_count,
                detail=(
                    "sample=" + ",".join(canary.sample_issuers[:10])
                    if canary.sample_issuers
                    else None
                ),
            )
        )
    except Exception as exc:  # noqa: BLE001 - adapter boundary fails closed
        checks.append(
            GateCheck(
                name="fundamentals_arq_two_quarter_canary",
                table=SharadarTable.FUNDAMENTALS.value,
                passed=False,
                expected=f">={minimum_arq_two_quarter_issuers}",
                actual=type(exc).__name__,
                detail="canary evidence failed closed",
            )
        )

    failure_count = sum(not check.passed for check in checks)
    instant = checked_at or datetime.now(UTC)
    if instant.tzinfo is None:
        raise ReadinessConfigurationError("checked_at must be timezone-aware")
    return ReadinessReport(
        format=READINESS_FORMAT,
        checked_at=instant.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        deployment=selected_route.deployment.value,
        database_name=selected_route.database_name,
        tables=tuple(table.value for table in required_tables),
        ready_for_prod_backfill=failure_count == 0,
        # A readiness report can never authorize a production mutation.
        prod_write_authorized=False,
        failed_checks=failure_count,
        checks=tuple(checks),
    )


def _table_checks(
    spec: TableGateSpec, evidence: TableEvidence
) -> list[GateCheck]:
    table = spec.table.value
    row_count_matches = (
        evidence.manifest_row_count is not None
        and evidence.manifest_row_count == evidence.stored_row_count
    )
    values = (
        (
            "published_manifest",
            bool(evidence.manifest_id) and evidence.manifest_published,
            "published manifest",
            evidence.manifest_id,
        ),
        ("nonempty_rows", evidence.stored_row_count > 0, ">0", evidence.stored_row_count),
        (
            "manifest_row_count",
            row_count_matches,
            evidence.manifest_row_count,
            evidence.stored_row_count,
        ),
        (
            "primary_key_not_null",
            evidence.primary_key_null_rows == 0,
            0,
            evidence.primary_key_null_rows,
        ),
        (
            "primary_key_unique",
            evidence.duplicate_primary_keys == 0,
            0,
            evidence.duplicate_primary_keys,
        ),
        (
            "schema_digest",
            evidence.actual_schema_digest == spec.schema_digest,
            spec.schema_digest,
            evidence.actual_schema_digest,
        ),
        (
            "pit_clock_fields_present",
            evidence.pit_clock_missing_rows == 0,
            0,
            evidence.pit_clock_missing_rows,
        ),
        (
            "pit_clock_order",
            evidence.pit_clock_order_violations == 0,
            0,
            evidence.pit_clock_order_violations,
        ),
        (
            "artifact_checksum",
            evidence.artifact_checksum_verified,
            True,
            evidence.artifact_checksum_verified,
        ),
        (
            "replay_idempotent",
            evidence.replay_verified,
            True,
            evidence.replay_verified,
        ),
        (
            "required_indexes",
            evidence.required_indexes_present,
            True,
            evidence.required_indexes_present,
        ),
        ("watermark", bool(evidence.watermark), "non-empty", evidence.watermark),
    )
    return [
        GateCheck(
            name=name,
            table=table,
            passed=passed,
            expected=expected,
            actual=actual,
        )
        for name, passed, expected, actual in values
    ]


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Hash an artifact in one bounded-memory pass."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def fundamentals_plan_tables() -> tuple[SharadarTable, ...]:
    """Catalog-ordered table list for the paid Fundamentals plan."""

    return tuple(
        table
        for table in SharadarTable
        if Plan.FUNDAMENTALS in table_spec(normalize_table(table)).plans
    )


def gate_specs_from_registry(registry: Any) -> tuple[TableGateSpec, ...]:
    """Adapt the immutable schema registry to the minimal readiness contract.

    The function intentionally relies only on the registry's public ``table``
    method and table-schema attributes, keeping the validator independent of the
    concrete registry class.
    """

    specs: list[TableGateSpec] = []
    # The storage manifest records this normalized fingerprint, not merely the
    # upstream HTTP payload digest.
    from .storage.artifacts import resolve_registry

    for table in fundamentals_plan_tables():
        schema = registry.table(table)
        storage_schema = resolve_registry(registry, table.value)
        primary_key = tuple(schema.primary_key)
        date_columns = set(schema.date_columns)
        clocks, order = _PIT_CLOCK_POLICY[table]
        missing_clocks = set(clocks) - date_columns
        if missing_clocks:
            raise ReadinessConfigurationError(
                f"{table.value}: registry is missing pinned PIT clocks"
            )
        specs.append(
            TableGateSpec(
                table=table,
                primary_key=primary_key,
                schema_digest=storage_schema.fingerprint,
                pit_clock_fields=clocks,
                pit_clock_order=order,
                required_indexes=(primary_key,),
            )
        )
    return tuple(specs)
