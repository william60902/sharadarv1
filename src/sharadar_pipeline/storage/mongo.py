"""Bounded MongoDB upserts for schema-admitted Sharadar rows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

from pymongo import ASCENDING, UpdateOne

from ..catalog import normalize_table
from .artifacts import normalize_row, resolve_registry


@dataclass(frozen=True, slots=True)
class MongoUpsertReceipt:
    table: str
    collection: str
    input_rows: int
    submitted_rows: int
    matched_count: int
    modified_count: int
    upserted_count: int
    batches: int


class MongoCurrentStore:
    """Persist query-ready rows by the registry's declared primary key."""

    def __init__(self, database: Any, *, batch_size: int = 1_000) -> None:
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size <= 0
        ):
            raise ValueError("batch_size must be a positive integer")
        self.database = database
        self.batch_size = batch_size
        self._ensured_indexes: set[tuple[str, str]] = set()

    @staticmethod
    def collection_name(table: str) -> str:
        # The database already identifies the vendor and deployment.  Keeping
        # collection names identical to Sharadar's public table names avoids
        # confusing schema/type admission with quantitative normalization.
        return normalize_table(table).value

    def ensure_indexes(self, table: str, registry: Any) -> tuple[str, ...]:
        exact_table = normalize_table(table).value
        view = resolve_registry(registry, exact_table)
        cache_key = (exact_table, view.fingerprint)
        if cache_key in self._ensured_indexes:
            return (
                "pk__" + "__".join(view.primary_keys),
                "source_sha256",
                "run_id",
            )
        collection = self.database[self.collection_name(exact_table)]
        primary_name = "pk__" + "__".join(view.primary_keys)
        names = [
            collection.create_index(
                [(key, ASCENDING) for key in view.primary_keys],
                name=primary_name,
                unique=True,
            ),
            collection.create_index(
                [("_storage.source_sha256", ASCENDING)],
                name="source_sha256",
            ),
            collection.create_index(
                [("_storage.run_id", ASCENDING)],
                name="run_id",
            ),
        ]
        self._ensured_indexes.add(cache_key)
        return tuple(names)

    def upsert_rows(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        registry: Any,
        *,
        run_id: str,
        source_sha256: str,
    ) -> MongoUpsertReceipt:
        exact_table = normalize_table(table).value
        view = resolve_registry(registry, exact_table)
        self.ensure_indexes(exact_table, registry)
        collection_name = self.collection_name(exact_table)
        collection = self.database[collection_name]
        input_rows = submitted_rows = matched = modified = upserted = batches = 0
        pending: list[Mapping[str, Any]] = []

        def flush() -> None:
            nonlocal submitted_rows, matched, modified, upserted, batches
            if not pending:
                return
            # Latest duplicate PK in an input batch wins deterministically.  This
            # avoids duplicate-key races inside unordered bulk_write.
            deduplicated: dict[tuple[Any, ...], dict[str, Any]] = {}
            for source in pending:
                normalized = _bson_safe(normalize_row(source, view))
                key = tuple(normalized[name] for name in view.primary_keys)
                deduplicated[key] = normalized
            now = datetime.now(UTC)
            operations = []
            for key, normalized in deduplicated.items():
                selector = dict(zip(view.primary_keys, key, strict=True))
                storage = {
                    "run_id": run_id,
                    "source_sha256": source_sha256,
                    "schema_fingerprint": view.fingerprint,
                    "stored_at": now,
                }
                operations.append(
                    UpdateOne(
                        selector,
                        {
                            "$set": {**normalized, "_storage": storage},
                            "$setOnInsert": {"_created_at": now},
                        },
                        upsert=True,
                    )
                )
            if operations:
                result = collection.bulk_write(operations, ordered=False)
                submitted_rows += len(operations)
                matched += int(getattr(result, "matched_count", 0))
                modified += int(getattr(result, "modified_count", 0))
                upserted += int(getattr(result, "upserted_count", 0))
                batches += 1
            pending.clear()

        for row in rows:
            input_rows += 1
            pending.append(row)
            if len(pending) >= self.batch_size:
                flush()
        flush()
        return MongoUpsertReceipt(
            table=exact_table,
            collection=collection_name,
            input_rows=input_rows,
            submitted_rows=submitted_rows,
            matched_count=matched,
            modified_count=modified,
            upserted_count=upserted,
            batches=batches,
        )


def _bson_safe(value: Any) -> Any:
    """Convert Python date values to BSON UTC datetimes without touching Parquet."""

    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if isinstance(value, Mapping):
        return {key: _bson_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_bson_safe(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_bson_safe(item) for item in value)
    return value
