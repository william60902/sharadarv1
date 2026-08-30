"""Immutable filesystem artifacts for the Sharadar vendor layer.

The store deliberately knows nothing about PDATA/PSTAGE/PMART.  It persists
vendor captures, normalized Parquet objects, run manifests, and watermarks
under a caller-selected DEV or PROD root.
"""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Final, Self

from ..catalog import normalize_table

DEFAULT_IO_CHUNK_SIZE: Final = 1024 * 1024
_SAFE_SUFFIX = re.compile(r"^\.[a-z0-9]{1,12}$")
_UNSUPPORTED_ATOMIC_FS_ERRNOS: Final = frozenset(
    {errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOSYS, errno.EINVAL}
)


class StorageArtifactError(RuntimeError):
    """Artifact could not be validated or durably promoted."""


@dataclass(frozen=True, slots=True)
class RawCaptureReceipt:
    table: str
    sha256: str
    byte_count: int
    artifact_path: Path
    replayed: bool


@dataclass(frozen=True, slots=True)
class ParquetArtifactReceipt:
    table: str
    sha256: str
    byte_count: int
    row_count: int
    artifact_path: Path
    replayed: bool


@dataclass(frozen=True, slots=True)
class RegistryView:
    headers: tuple[str, ...]
    types: Mapping[str, Any]
    primary_keys: tuple[str, ...]
    nullable: Mapping[str, bool]
    version: str
    fingerprint: str
    resource_sha256: str | None


def resolve_registry(registry: Any, table: str) -> RegistryView:
    """Resolve both the legacy duck type and the canonical TableSchema API."""

    spec = _resolve_registry_spec(registry, table)
    headers = _read_field(spec, "ordered_headers", "headers")
    primary_keys = _read_field(spec, "primary_key", "primary_keys")
    if not _is_string_sequence(headers) or not headers:
        raise ValueError("registry ordered_headers/headers must be non-empty")
    if not _is_string_sequence(primary_keys) or not primary_keys:
        raise ValueError("registry primary_key/primary_keys must be non-empty")
    exact_headers = tuple(headers)
    exact_primary_keys = tuple(primary_keys)
    if len(exact_headers) != len(set(exact_headers)):
        raise ValueError("registry headers must be unique")
    if not set(exact_primary_keys).issubset(exact_headers):
        raise ValueError("registry primary keys must be declared headers")

    types = _read_field(spec, "types", default=None)
    if types is None:
        types = _types_from_columns(_read_field(spec, "columns", default=None))
    if not isinstance(types, Mapping):
        raise TypeError("registry types/columns must resolve to a mapping")
    missing_types = set(exact_headers).difference(types)
    if missing_types:
        raise ValueError(f"registry types missing columns: {sorted(missing_types)!r}")
    exact_types = {header: types[header] for header in exact_headers}
    nullable = _nullability_from_columns(
        _read_field(spec, "columns", default=None), exact_headers, exact_primary_keys
    )

    version_value = _read_field(
        spec,
        "version",
        "schema_version",
        default=_read_field(
            registry, "version", "schema_version", default="unversioned"
        ),
    )
    version = str(getattr(version_value, "value", version_value))
    resource_sha256_value = _read_field(
        registry,
        "resource_sha256",
        "registry_resource_sha256",
        default=_read_field(spec, "resource_sha256", default=None),
    )
    resource_sha256 = (
        str(resource_sha256_value) if resource_sha256_value is not None else None
    )
    canonical = {
        "headers": exact_headers,
        "primary_keys": exact_primary_keys,
        "types": {key: _stable_type_name(value) for key, value in exact_types.items()},
        "nullable": nullable,
        "version": version,
    }
    fingerprint = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return RegistryView(
        headers=exact_headers,
        types=exact_types,
        primary_keys=exact_primary_keys,
        nullable=nullable,
        version=version,
        fingerprint=fingerprint,
        resource_sha256=resource_sha256,
    )


class ArtifactStore:
    """Content-addressed, crash-safe artifact store rooted at one deployment."""

    def __init__(
        self, root: str | os.PathLike[str], *, chunk_size: int = DEFAULT_IO_CHUNK_SIZE
    ) -> None:
        if (
            not isinstance(chunk_size, int)
            or isinstance(chunk_size, bool)
            or chunk_size <= 0
        ):
            raise ValueError("chunk_size must be a positive integer")
        self.root = Path(root).expanduser().resolve()
        self.chunk_size = chunk_size

    def capture_file(
        self,
        table: str,
        source_path: str | os.PathLike[str],
        *,
        suffix: str | None = None,
    ) -> RawCaptureReceipt:
        source = Path(source_path)
        exact_suffix = (
            suffix if suffix is not None else (source.suffix.lower() or ".bin")
        )
        with source.open("rb") as handle:
            return self.capture_chunks(
                table,
                iter(lambda: handle.read(self.chunk_size), b""),
                suffix=exact_suffix,
            )

    def capture_chunks(
        self,
        table: str,
        chunks: Iterable[bytes],
        *,
        suffix: str = ".bin",
    ) -> RawCaptureReceipt:
        exact_table = normalize_table(table).value
        exact_suffix = _validate_suffix(suffix)
        temp_dir = self.root / ".tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        byte_count = 0
        fd, temp_name = tempfile.mkstemp(prefix=f"raw-{exact_table}-", dir=temp_dir)
        temp_path = Path(temp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise TypeError("raw capture chunks must be bytes")
                    if not chunk:
                        continue
                    handle.write(chunk)
                    digest.update(chunk)
                    byte_count += len(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            hexdigest = digest.hexdigest()
            target = (
                self.root
                / "raw"
                / exact_table
                / "sha256"
                / hexdigest[:2]
                / f"{hexdigest}{exact_suffix}"
            )
            replayed = _promote_immutable(temp_path, target)
            return RawCaptureReceipt(
                table=exact_table,
                sha256=hexdigest,
                byte_count=byte_count,
                artifact_path=target,
                replayed=replayed,
            )
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise

    def parquet_writer(
        self,
        table: str,
        registry: Any,
        run_id: str,
        *,
        compression: str = "zstd",
        row_group_size: int = 50_000,
    ) -> AtomicParquetWriter:
        return AtomicParquetWriter(
            self,
            table,
            resolve_registry(registry, normalize_table(table).value),
            run_id,
            compression=compression,
            row_group_size=row_group_size,
        )

    def run_manifest_path(self, table: str, run_id: str) -> Path:
        return (
            self.root
            / "runs"
            / normalize_table(table).value
            / f"{_safe_id(run_id)}.json"
        )

    def read_run_manifest(self, table: str, run_id: str) -> dict[str, Any] | None:
        path = self.run_manifest_path(table, run_id)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            raise StorageArtifactError("run manifest must be a JSON object")
        return value

    def write_run_manifest(
        self, table: str, run_id: str, payload: Mapping[str, Any]
    ) -> Path:
        path = self.run_manifest_path(table, run_id)
        return _write_json_immutable(path, payload)

    def write_watermark(self, table: str, payload: Mapping[str, Any]) -> Path:
        path = self.root / "watermarks" / f"{normalize_table(table).value}.json"
        _write_json_replace(path, payload)
        return path


class AtomicParquetWriter:
    """Incremental Parquet writer with bounded input batches and atomic commit."""

    def __init__(
        self,
        store: ArtifactStore,
        table: str,
        registry: RegistryView,
        run_id: str,
        *,
        compression: str,
        row_group_size: int,
    ) -> None:
        if (
            not isinstance(row_group_size, int)
            or isinstance(row_group_size, bool)
            or row_group_size <= 0
        ):
            raise ValueError("row_group_size must be a positive integer")
        self._store = store
        self.table = normalize_table(table).value
        self.registry = registry
        self.run_id = _safe_id(run_id)
        self._compression = compression
        self._row_group_size = row_group_size
        self._writer: Any | None = None
        self._schema: Any | None = None
        self._row_count = 0
        self._closed = False
        temp_dir = store.root / ".tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(
            prefix=f"parquet-{self.table}-", suffix=".parquet", dir=temp_dir
        )
        os.close(fd)
        self._temp_path = Path(name)

    @property
    def row_count(self) -> int:
        return self._row_count

    def write_rows(self, rows: Sequence[Mapping[str, Any]]) -> int:
        if self._closed:
            raise StorageArtifactError("Parquet writer is closed")
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
            raise TypeError("write_rows requires one bounded sequence batch")
        if len(rows) > self._row_group_size:
            raise ValueError("row batch exceeds configured row_group_size")
        if not rows:
            return 0
        pa, pq = _load_pyarrow()
        if self._schema is None:
            self._schema = _arrow_schema(pa, self.registry)
            self._writer = pq.ParquetWriter(
                self._temp_path,
                self._schema,
                compression=self._compression,
                use_dictionary=True,
            )
        materialized: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError("normalized rows must be mappings")
            materialized.append(normalize_row(row, self.registry))
        table = pa.Table.from_pylist(materialized, schema=self._schema)
        self._writer.write_table(table, row_group_size=self._row_group_size)
        self._row_count += len(materialized)
        return len(materialized)

    def commit(self) -> ParquetArtifactReceipt:
        if self._closed:
            raise StorageArtifactError("Parquet writer is closed")
        try:
            if self._writer is None:
                pa, pq = _load_pyarrow()
                self._schema = _arrow_schema(pa, self.registry)
                self._writer = pq.ParquetWriter(
                    self._temp_path,
                    self._schema,
                    compression=self._compression,
                    use_dictionary=True,
                )
            self._writer.close()
            self._writer = None
            with self._temp_path.open("rb") as handle:
                os.fsync(handle.fileno())
            digest, byte_count = _hash_file(self._temp_path, self._store.chunk_size)
            target = (
                self._store.root
                / "normalized"
                / self.table
                / "sha256"
                / digest[:2]
                / f"{digest}.parquet"
            )
            replayed = _promote_immutable(self._temp_path, target)
            self._closed = True
            return ParquetArtifactReceipt(
                table=self.table,
                sha256=digest,
                byte_count=byte_count,
                row_count=self._row_count,
                artifact_path=target,
                replayed=replayed,
            )
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        if self._writer is not None:
            with suppress(Exception):
                self._writer.close()
            self._writer = None
        self._temp_path.unlink(missing_ok=True)
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._closed:
            self.abort()


def receipt_dict(receipt: RawCaptureReceipt | ParquetArtifactReceipt) -> dict[str, Any]:
    value = asdict(receipt)
    value["artifact_path"] = str(value["artifact_path"])
    return value


def normalize_row(row: Mapping[str, Any], registry: RegistryView) -> dict[str, Any]:
    """Strictly coerce REST/CSV values using the committed PostgreSQL policy."""

    if not isinstance(row, Mapping):
        raise TypeError("normalized rows must be mappings")
    extras = set(row).difference(registry.headers)
    if extras:
        raise ValueError(f"normalized row has undeclared columns: {sorted(extras)!r}")
    output: dict[str, Any] = {}
    for name in registry.headers:
        value = row.get(name)
        if value == "":
            value = None
        if value is None:
            if not registry.nullable[name]:
                raise ValueError(f"non-nullable column {name!r} cannot be null")
            output[name] = None
            continue
        output[name] = _coerce_value(value, registry.types[name], column=name)
    return output


def _resolve_registry_spec(registry: Any, table: str) -> Any:
    for method_name in ("schema_for", "get_schema", "table_schema", "table"):
        method = getattr(registry, method_name, None)
        if callable(method):
            return method(table)
    if isinstance(registry, Mapping):
        if table in registry:
            return registry[table]
        canonical_table = normalize_table(table)
        if canonical_table in registry:
            return registry[canonical_table]
        # A direct schema mapping is also a valid duck type.
        if "ordered_headers" in registry or "headers" in registry:
            return registry
    if hasattr(registry, "ordered_headers") or hasattr(registry, "headers"):
        return registry
    try:
        return registry[table]
    except (KeyError, TypeError, AttributeError):
        raise ValueError(f"registry has no schema for {table!r}") from None


def _read_field(spec: Any, *names: str, default: Any = ...) -> Any:
    for name in names:
        if isinstance(spec, Mapping) and name in spec:
            return spec[name]
        if hasattr(spec, name):
            return getattr(spec, name)
    if default is not ...:
        return default
    raise ValueError(f"registry schema missing {'/'.join(names)}")


def _types_from_columns(columns: Any) -> dict[str, Any]:
    if isinstance(columns, Mapping):
        return {str(name): _column_type(column) for name, column in columns.items()}
    if isinstance(columns, Sequence) and not isinstance(
        columns, (str, bytes, bytearray)
    ):
        output: dict[str, Any] = {}
        for column in columns:
            name = _read_field(column, "name", "column", "header")
            output[str(name)] = _column_type(column)
        return output
    raise ValueError("registry columns must be a mapping or sequence")


def _nullability_from_columns(
    columns: Any, headers: tuple[str, ...], primary_keys: tuple[str, ...]
) -> dict[str, bool]:
    output = {name: name not in primary_keys for name in headers}
    if isinstance(columns, Mapping):
        items = columns.items()
    elif isinstance(columns, Sequence) and not isinstance(
        columns, (str, bytes, bytearray)
    ):
        items = (
            (_read_field(column, "name", "column", "header"), column)
            for column in columns
        )
    else:
        return output
    for name, column in items:
        nullable = _read_field(column, "nullable", default=None)
        if nullable is not None:
            if type(nullable) is not bool:
                raise ValueError("registry column nullable must be boolean")
            output[str(name)] = nullable
    return output


def _column_type(column: Any) -> Any:
    if isinstance(column, (str, type)):
        return column
    return _read_field(
        column,
        "storage_type",
        "parquet_type",
        "postgres_type",
        "dtype",
        "python_type",
        "type",
        "logical_type",
    )


def _stable_type_name(value: Any) -> str:
    exact = getattr(value, "value", value)
    if isinstance(exact, type):
        return f"{exact.__module__}.{exact.__qualname__}"
    return str(exact)


def _coerce_value(value: Any, declared_type: Any, *, column: str) -> Any:
    exact = getattr(declared_type, "value", declared_type)
    if exact is str:
        key = "text"
    elif exact is int:
        key = "bigint"
    elif exact is float:
        key = "double precision"
    elif exact is bool:
        key = "boolean"
    elif exact is date:
        key = "date"
    elif exact is datetime:
        key = "timestamp"
    else:
        key = str(exact).strip().lower().replace("_", " ")
    try:
        if key in {"string", "str", "text", "character varying", "varchar"}:
            if type(value) is not str:
                raise TypeError
            return value
        if key in {"integer", "int", "int64", "long", "bigint"}:
            if isinstance(value, bool):
                raise TypeError
            if isinstance(value, int):
                return value
            if isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
                return int(value)
            raise TypeError
        if key in {
            "float",
            "float64",
            "double",
            "double precision",
            "number",
            "numeric",
        }:
            if isinstance(value, bool):
                raise TypeError
            if isinstance(value, (int, float, str)):
                number = float(value)
                if math.isfinite(number):
                    return number
            raise TypeError
        if key in {"bool", "boolean"}:
            if type(value) is bool:
                return value
            if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
                return value.strip().lower() == "true"
            raise TypeError
        if key == "date":
            if isinstance(value, datetime):
                return value.date()
            if type(value) is date:
                return value
            if isinstance(value, str):
                return date.fromisoformat(value)
            raise TypeError
        if key in {"datetime", "timestamp", "timestamp utc"}:
            if isinstance(value, datetime):
                return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
            if isinstance(value, str):
                parsed = datetime.fromisoformat(value)
                return (
                    parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
                )
            raise TypeError
    except (TypeError, ValueError, OverflowError):
        raise ValueError(
            f"column {column!r} cannot be coerced to {_stable_type_name(declared_type)!r}"
        ) from None
    raise ValueError(
        f"unsupported storage type for column {column!r}: {_stable_type_name(declared_type)!r}"
    )


def _arrow_schema(pa: Any, registry: RegistryView) -> Any:
    return pa.schema(
        [
            pa.field(
                name,
                _arrow_type(pa, registry.types[name]),
                nullable=registry.nullable[name],
            )
            for name in registry.headers
        ]
    )


def _arrow_type(pa: Any, value: Any) -> Any:
    if hasattr(value, "equals") and value.__class__.__module__.startswith("pyarrow"):
        return value
    exact = getattr(value, "value", value)
    if exact is str:
        return pa.string()
    if exact is int:
        return pa.int64()
    if exact is float:
        return pa.float64()
    if exact is bool:
        return pa.bool_()
    if exact is date:
        return pa.date32()
    if exact is datetime:
        return pa.timestamp("us", tz="UTC")
    key = str(exact).strip().lower().replace("_", "")
    aliases = {
        "string": pa.string,
        "str": pa.string,
        "text": pa.string,
        "integer": pa.int64,
        "int": pa.int64,
        "int64": pa.int64,
        "long": pa.int64,
        "bigint": pa.int64,
        "float": pa.float64,
        "float64": pa.float64,
        "double": pa.float64,
        "double precision": pa.float64,
        "number": pa.float64,
        "bool": pa.bool_,
        "boolean": pa.bool_,
        "date": pa.date32,
    }
    if key in aliases:
        return aliases[key]()
    if key in {"datetime", "timestamp", "timestamputc"}:
        return pa.timestamp("us", tz="UTC")
    raise ValueError(f"unsupported Parquet type: {_stable_type_name(value)!r}")


def _load_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise StorageArtifactError(
            "Parquet storage requires pyarrow; install the project storage dependencies"
        ) from None
    return pa, pq


def _hash_file(path: Path, chunk_size: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _promote_immutable(temp_path: Path, target: Path) -> bool:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(temp_path, target)
    except FileExistsError:
        _verify_same_content(temp_path, target)
        replayed = True
    except OSError as exc:
        if exc.errno not in _UNSUPPORTED_ATOMIC_FS_ERRNOS:
            raise
        # SMB commonly rejects hard links.  Both paths are deliberately created
        # below the same root, so os.replace remains a same-filesystem atomic
        # promotion.  A concurrent same-digest writer can only replace the target
        # with identical bytes; pre-existing targets are verified first.
        if target.exists():
            _verify_same_content(temp_path, target)
            replayed = True
        else:
            os.replace(temp_path, target)
            replayed = False
            _fsync_directory(target.parent)
    else:
        replayed = False
        _fsync_directory(target.parent)
    finally:
        temp_path.unlink(missing_ok=True)
    return replayed


def _write_json_immutable(path: Path, payload: Mapping[str, Any]) -> Path:
    encoded = _json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _write_temp(path.parent, encoded)
    try:
        try:
            os.link(temp, path)
        except FileExistsError:
            with path.open("rb") as handle:
                if handle.read() != encoded:
                    raise StorageArtifactError("immutable manifest collision")
        except OSError as exc:
            if exc.errno not in _UNSUPPORTED_ATOMIC_FS_ERRNOS:
                raise
            if path.exists():
                with path.open("rb") as handle:
                    if handle.read() != encoded:
                        raise StorageArtifactError("immutable manifest collision")
            else:
                os.replace(temp, path)
                _fsync_directory(path.parent)
        else:
            _fsync_directory(path.parent)
    finally:
        temp.unlink(missing_ok=True)
    return path


def _write_json_replace(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = _json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _write_temp(path.parent, encoded)
    try:
        os.replace(temp, path)
        _fsync_directory(path.parent)
    finally:
        temp.unlink(missing_ok=True)


def _write_temp(parent: Path, value: bytes) -> Path:
    fd, name = tempfile.mkstemp(prefix="json-", dir=parent)
    path = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=_json_default,
        ).encode("utf-8")
        + b"\n"
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"cannot JSON encode {type(value).__name__}")


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as exc:
        if exc.errno in _UNSUPPORTED_ATOMIC_FS_ERRNOS:
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError as exc:
            if exc.errno not in _UNSUPPORTED_ATOMIC_FS_ERRNOS:
                raise
    finally:
        os.close(descriptor)


def _verify_same_content(left: Path, right: Path) -> None:
    if left.stat().st_size != right.stat().st_size:
        raise StorageArtifactError("content-addressed artifact collision")
    left_sha, _ = _hash_file(left, DEFAULT_IO_CHUNK_SIZE)
    right_sha, _ = _hash_file(right, DEFAULT_IO_CHUNK_SIZE)
    if left_sha != right_sha:
        raise StorageArtifactError("content-addressed artifact collision")


def _validate_suffix(value: str) -> str:
    if type(value) is not str or not _SAFE_SUFFIX.fullmatch(value.lower()):
        raise ValueError("artifact suffix must be a short lowercase extension")
    return value.lower()


def _safe_id(value: str) -> str:
    if type(value) is not str or not re.fullmatch(
        r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", value
    ):
        raise ValueError("invalid storage identifier")
    return value


def _is_string_sequence(value: Any) -> bool:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes, bytearray))
        and all(type(item) is str and item for item in value)
    )
