"""Reusable, redacting HTTP client for the direct Sharadar API.

Only the first-party API request receives ``x-api-key``.  Redirects are disabled
for every request so the credential cannot be forwarded to a bulk download host.
Bulk streaming intentionally lives in :mod:`sharadar_pipeline.bulk`.
"""

from __future__ import annotations

import email.utils
import hashlib
import json
import math
import random
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum, StrEnum
from typing import Any, Protocol
from urllib.parse import urlparse

import requests

from .errors import (
    SharadarAuthenticationError,
    SharadarConfigurationError,
    SharadarDecodeError,
    SharadarEntitlementError,
    SharadarRateLimitError,
    SharadarRedirectError,
    SharadarResponseError,
    SharadarTransportError,
)

try:  # Catalog is maintained separately; keep the HTTP layer duck-type friendly.
    from .catalog import normalize_table as _catalog_normalize_table
except ImportError:  # pragma: no cover - used only during isolated bootstrap
    _catalog_normalize_table = None


API_BASE_URL = "https://api.sharadar.com/v1.0"
USER_AGENT = "Medina-SharadarV1/0.1"
_TABLE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_AUTH_PARAM_NAMES = frozenset(
    {
        "api_key",
        "apikey",
        "x-api-key",
        "authorization",
        "proxy-authorization",
        "cookie",
        "token",
    }
)
_RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
_SCHEMA_FORMATS = frozenset({"postgres", "sqlite", "mysql"})
_HISTORY_VALUES = frozenset({"5", "10", "full"})
_FIELD_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TICKER_PATTERN = re.compile(r"^[^\s,]+$")
_QUERY_CONTROL_FIELDS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "x-api-key",
        "format",
        "ticker",
        "from",
        "to",
        "fields",
        "sort",
        "limit",
        "skip",
        "offset",
        "years",
        "status",
    }
)
DEFAULT_MAX_PAGES = 1_000


class ResponseLike(Protocol):
    status_code: int
    headers: Mapping[str, str]
    text: str

    def json(self) -> Any: ...


class SessionLike(Protocol):
    def request(self, method: str, url: str, **kwargs: Any) -> ResponseLike: ...


class FilterOperator(StrEnum):
    """Comparison operators documented by the Sharadar data endpoint."""

    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"

    @property
    def suffix(self) -> str:
        return "" if self is FilterOperator.EQ else f".{self.value}"


class SortDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


def _typed_enum(value: object, enum_type: type[StrEnum], label: str) -> StrEnum:
    try:
        return enum_type(value)
    except (TypeError, ValueError):
        raise SharadarConfigurationError(f"invalid {label}") from None


def _field_name(value: object, *, allow_control: bool = False) -> str:
    if not isinstance(value, str) or not _FIELD_PATTERN.fullmatch(value):
        raise SharadarConfigurationError("query field names must be simple identifiers")
    if not allow_control and value.lower() in _QUERY_CONTROL_FIELDS:
        raise SharadarConfigurationError("query filter cannot override control parameters")
    return value


def _ordered_strings(
    value: object,
    *,
    label: str,
    pattern: re.Pattern[str] | None = None,
) -> tuple[str, ...]:
    if value in (None, ()):
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise SharadarConfigurationError(f"{label} must be an ordered sequence")
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item or item != item.strip():
            raise SharadarConfigurationError(f"{label} must contain non-empty strings")
        if pattern is not None and not pattern.fullmatch(item):
            raise SharadarConfigurationError(f"invalid value in {label}")
        if item in seen:
            raise SharadarConfigurationError(f"{label} cannot contain duplicates")
        seen.add(item)
        result.append(item)
    if not result:
        raise SharadarConfigurationError(f"{label} cannot be empty")
    return tuple(result)


def _iso_date(value: date | str | None, *, label: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise SharadarConfigurationError(f"{label} must be a date, not a timestamp")
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not _DATE_PATTERN.fullmatch(value):
        raise SharadarConfigurationError(f"{label} must use YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise SharadarConfigurationError(f"{label} is not a real calendar date") from None


def _bounded_int(value: object, *, label: str, minimum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        comparator = "positive" if minimum == 1 else "non-negative"
        raise SharadarConfigurationError(f"{label} must be a {comparator} integer")
    return value


def _filter_value(value: object) -> object:
    if value is None or isinstance(value, bool):
        raise SharadarConfigurationError("filter value cannot be null or boolean")
    if isinstance(value, float) and not math.isfinite(value):
        raise SharadarConfigurationError("filter value must be finite")
    if isinstance(value, (set, frozenset)):
        raise SharadarConfigurationError("filter multi-values must be ordered")
    if isinstance(value, (list, tuple)):
        if not value:
            raise SharadarConfigurationError("filter multi-value cannot be empty")
        normalized = tuple(_filter_value(item) for item in value)
        if len(normalized) != len(set(normalized)):
            raise SharadarConfigurationError(
                "filter multi-value cannot contain duplicates"
            )
        return normalized
    if isinstance(value, str) and not value:
        raise SharadarConfigurationError("filter value cannot be empty")
    if isinstance(value, datetime):
        raise SharadarConfigurationError("filter dates cannot be timestamps")
    if isinstance(value, date):
        return value.isoformat()
    return value


@dataclass(frozen=True, slots=True)
class QueryFilter:
    field: str
    operator: FilterOperator | str
    value: object

    def __post_init__(self) -> None:
        normalized_field = _field_name(self.field)
        object.__setattr__(self, "field", normalized_field)
        object.__setattr__(
            self,
            "operator",
            _typed_enum(self.operator, FilterOperator, "filter operator"),
        )
        normalized_value = _filter_value(self.value)
        if normalized_field in {
            "date",
            "lastupdated",
            "calendardate",
            "reportperiod",
        }:
            date_values = (
                normalized_value
                if isinstance(normalized_value, tuple)
                else (normalized_value,)
            )
            parsed_values = tuple(
                _iso_date(item, label=normalized_field) for item in date_values
            )
            normalized_value = (
                tuple(item.isoformat() for item in parsed_values if item is not None)
                if isinstance(normalized_value, tuple)
                else parsed_values[0].isoformat()  # type: ignore[union-attr]
            )
        object.__setattr__(self, "value", normalized_value)

    @property
    def parameter(self) -> str:
        return f"{self.field}{self.operator.suffix}"


@dataclass(frozen=True, slots=True)
class SortSpec:
    field: str
    direction: SortDirection | str = SortDirection.ASC

    def __post_init__(self) -> None:
        object.__setattr__(self, "field", _field_name(self.field, allow_control=True))
        object.__setattr__(
            self,
            "direction",
            _typed_enum(self.direction, SortDirection, "sort direction"),
        )

    @property
    def wire_value(self) -> str:
        return f"{self.field}.{self.direction.value}"


@dataclass(frozen=True, slots=True)
class QuerySpec:
    """Validated, deterministic input for a bounded Sharadar REST query."""

    table: object
    output_format: object = "json"
    tickers: Sequence[str] = ()
    from_date: date | str | None = None
    to_date: date | str | None = None
    fields: Sequence[str] = ()
    filters: Sequence[QueryFilter] = ()
    sort: SortSpec | None = None
    limit: int | None = None
    skip: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "table", _normalize_table(self.table))
        object.__setattr__(
            self, "output_format", _normalize_output_format(self.output_format)
        )
        object.__setattr__(
            self,
            "tickers",
            _ordered_strings(self.tickers, label="tickers", pattern=_TICKER_PATTERN),
        )
        normalized_fields = _ordered_strings(self.fields, label="fields")
        for item in normalized_fields:
            _field_name(item, allow_control=True)
        object.__setattr__(self, "fields", normalized_fields)
        start = _iso_date(self.from_date, label="from_date")
        end = _iso_date(self.to_date, label="to_date")
        if start is not None and end is not None and start > end:
            raise SharadarConfigurationError("from_date cannot be after to_date")
        object.__setattr__(self, "from_date", start)
        object.__setattr__(self, "to_date", end)

        if isinstance(self.filters, (str, bytes, set, frozenset)) or not isinstance(
            self.filters, Sequence
        ):
            raise SharadarConfigurationError("filters must be an ordered sequence")
        filters = tuple(self.filters)
        if any(not isinstance(item, QueryFilter) for item in filters):
            raise SharadarConfigurationError("filters must contain QueryFilter values")
        parameters = [item.parameter for item in filters]
        if len(parameters) != len(set(parameters)):
            raise SharadarConfigurationError("query filters cannot contain duplicates")
        if start is not None and any(
            item.field == "date" and item.operator in {FilterOperator.GT, FilterOperator.GTE}
            for item in filters
        ):
            raise SharadarConfigurationError("from_date conflicts with a lower date filter")
        if end is not None and any(
            item.field == "date" and item.operator in {FilterOperator.LT, FilterOperator.LTE}
            for item in filters
        ):
            raise SharadarConfigurationError("to_date conflicts with an upper date filter")
        object.__setattr__(self, "filters", filters)

        if self.sort is not None and not isinstance(self.sort, SortSpec):
            raise SharadarConfigurationError("sort must be a SortSpec")
        if self.limit is not None:
            object.__setattr__(
                self, "limit", _bounded_int(self.limit, label="limit", minimum=1)
            )
        object.__setattr__(
            self, "skip", _bounded_int(self.skip, label="skip", minimum=0)
        )

    def parameters(self) -> dict[str, object]:
        params: dict[str, object] = {"format": self.output_format}
        if self.tickers:
            params["ticker"] = ",".join(self.tickers)
        if self.from_date is not None:
            params["from"] = self.from_date.isoformat()
        if self.to_date is not None:
            params["to"] = self.to_date.isoformat()
        if self.fields:
            params["fields"] = ",".join(self.fields)
        for item in self.filters:
            value = item.value
            params[item.parameter] = (
                ",".join(_plain_value(part) for part in value)
                if isinstance(value, tuple)
                else value
            )
        if self.sort is not None:
            params["sort"] = self.sort.wire_value
        if self.limit is not None:
            params["limit"] = self.limit
        if self.skip:
            params["skip"] = self.skip
        return params


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry defaults for small REST and metadata requests."""

    attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    retryable_statuses: frozenset[int] = field(default=_RETRYABLE_STATUSES)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.attempts, int)
            or isinstance(self.attempts, bool)
            or self.attempts < 1
        ):
            raise ValueError("retry attempts must be at least one")
        for value in (self.base_delay_seconds, self.max_delay_seconds):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError("retry delays must be finite and non-negative")
        if not isinstance(self.retryable_statuses, frozenset) or any(
            not isinstance(status, int) or isinstance(status, bool)
            for status in self.retryable_statuses
        ):
            raise ValueError("retryable statuses must be a frozenset of integers")


@dataclass(frozen=True, slots=True, repr=False)
class BulkRedirect:
    """A short-lived bulk location whose repr/string form never reveals it."""

    location: str = field(repr=False)
    table: str
    history: str

    @property
    def url(self) -> str:
        """Compatibility alias for downloaders that use ``redirect.url``."""

        return self.location

    def __repr__(self) -> str:
        return (
            f"BulkRedirect(table={self.table!r}, history={self.history!r}, "
            "location='<redacted-url>')"
        )

    __str__ = __repr__


def _plain_value(value: object) -> str:
    if isinstance(value, Enum) or hasattr(value, "value"):
        value = value.value
    return str(value)


def _normalize_table(table: object) -> str:
    if _catalog_normalize_table is not None:
        try:
            normalized = _catalog_normalize_table(table)
        except (TypeError, ValueError, KeyError):
            raise SharadarConfigurationError("unsupported Sharadar table") from None
        normalized = _plain_value(normalized)
    else:
        normalized = _plain_value(table).strip()
    if not _TABLE_PATTERN.fullmatch(normalized):
        raise SharadarConfigurationError("invalid Sharadar table name")
    return normalized.lower()


def _normalize_history(history: object) -> str:
    value = _plain_value(history).strip().lower()
    # Be compatible with enums named FIVE_YEARS / TEN_YEARS / FULL_HISTORY.
    aliases = {
        "5_years": "5",
        "five_years": "5",
        "10_years": "10",
        "ten_years": "10",
        "full_history": "full",
    }
    value = aliases.get(value, value)
    if value not in _HISTORY_VALUES:
        raise SharadarConfigurationError("history must be 5, 10, or full")
    return value


def _normalize_output_format(output_format: object) -> str:
    value = _plain_value(output_format).strip().lower()
    if value not in {"json", "csv"}:
        raise SharadarConfigurationError("output format must be json or csv")
    return value


def _safe_params(params: Mapping[str, object] | None) -> dict[str, object]:
    result: dict[str, object] = {}
    for raw_key, raw_value in (params or {}).items():
        key = str(raw_key)
        if key.strip().lower() in _AUTH_PARAM_NAMES:
            raise SharadarConfigurationError(
                "authentication parameters are forbidden; the client uses x-api-key"
            )
        if raw_value is None:
            continue
        if isinstance(raw_value, Enum):
            result[key] = raw_value.value
        elif isinstance(raw_value, float) and not math.isfinite(raw_value):
            raise SharadarConfigurationError("raw parameter values must be finite")
        elif isinstance(raw_value, (set, frozenset)):
            raise SharadarConfigurationError(
                "raw parameter multi-values must use an ordered sequence"
            )
        elif isinstance(raw_value, (list, tuple)):
            if any(
                isinstance(item, float) and not math.isfinite(item)
                for item in raw_value
            ):
                raise SharadarConfigurationError(
                    "raw parameter multi-values must be finite"
                )
            if key.lower() in {"ticker", "fields"} and any(
                not isinstance(item, str) for item in raw_value
            ):
                raise SharadarConfigurationError(
                    f"{key} must contain strings"
                )
            values = [_plain_value(item) for item in raw_value]
            if not values or any(not value for value in values):
                raise SharadarConfigurationError(
                    "raw parameter multi-values cannot be empty"
                )
            if len(values) != len(set(values)):
                raise SharadarConfigurationError(
                    "raw parameter multi-values cannot contain duplicates"
                )
            result[key] = ",".join(values)
        else:
            result[key] = raw_value
    _validate_raw_params(result)
    return result


def _validate_raw_params(params: dict[str, object]) -> None:
    """Validate the exploratory lane's common contract before network I/O."""

    lower_keys = {key.lower(): key for key in params}
    if "skip" in lower_keys and "offset" in lower_keys:
        raise SharadarConfigurationError("skip and offset cannot be combined")

    for name in ("limit", "skip", "offset"):
        if name in lower_keys:
            minimum = 1 if name == "limit" else 0
            params[lower_keys[name]] = _bounded_int(
                params[lower_keys[name]], label=name, minimum=minimum
            )

    parsed_dates: dict[str, date] = {}
    for name in ("from", "to"):
        if name in lower_keys:
            parsed = _iso_date(params[lower_keys[name]], label=name)  # type: ignore[arg-type]
            assert parsed is not None
            parsed_dates[name] = parsed
            params[lower_keys[name]] = parsed.isoformat()
    if (
        "from" in parsed_dates
        and "to" in parsed_dates
        and parsed_dates["from"] > parsed_dates["to"]
    ):
        raise SharadarConfigurationError("from cannot be after to")

    if "ticker" in lower_keys:
        raw_tickers = params[lower_keys["ticker"]]
        if not isinstance(raw_tickers, str):
            raise SharadarConfigurationError("ticker must be text or an ordered sequence")
        tickers = raw_tickers.split(",")
        if (
            any(not ticker or ticker != ticker.strip() or not _TICKER_PATTERN.fullmatch(ticker)
                for ticker in tickers)
            or len(tickers) != len(set(tickers))
        ):
            raise SharadarConfigurationError("ticker values are empty, invalid, or duplicated")

    if "fields" in lower_keys:
        raw_fields = params[lower_keys["fields"]]
        if not isinstance(raw_fields, str):
            raise SharadarConfigurationError("fields must be text or an ordered sequence")
        fields = raw_fields.split(",")
        if (
            any(not _FIELD_PATTERN.fullmatch(item) for item in fields)
            or len(fields) != len(set(fields))
        ):
            raise SharadarConfigurationError("fields are empty, invalid, or duplicated")

    if "sort" in lower_keys:
        raw_sort = params[lower_keys["sort"]]
        if not isinstance(raw_sort, str):
            raise SharadarConfigurationError("sort must be '<field>.asc|desc'")
        parts = raw_sort.rsplit(".", 1)
        if (
            len(parts) != 2
            or not _FIELD_PATTERN.fullmatch(parts[0])
            or parts[1] not in {"asc", "desc"}
        ):
            raise SharadarConfigurationError("sort must be '<field>.asc|desc'")

    date_filter_keys: set[str] = set()
    for key, value in list(params.items()):
        lower = key.lower()
        if "." in lower:
            field_name, suffix = lower.rsplit(".", 1)
            if not _FIELD_PATTERN.fullmatch(field_name) or suffix not in {
                "gt",
                "gte",
                "lt",
                "lte",
            }:
                raise SharadarConfigurationError("unsupported raw filter operator")
            if field_name in {"date", "lastupdated", "calendardate", "reportperiod"}:
                date_filter_keys.add(lower)
                parsed = _iso_date(value, label=key)  # type: ignore[arg-type]
                assert parsed is not None
                params[key] = parsed.isoformat()
        elif lower in {"date", "lastupdated", "calendardate", "reportperiod"}:
            parsed = _iso_date(value, label=key)  # type: ignore[arg-type]
            assert parsed is not None
            params[key] = parsed.isoformat()

    if "from" in parsed_dates and date_filter_keys.intersection({"date.gt", "date.gte"}):
        raise SharadarConfigurationError("from conflicts with a lower date filter")
    if "to" in parsed_dates and date_filter_keys.intersection({"date.lt", "date.lte"}):
        raise SharadarConfigurationError("to conflicts with an upper date filter")


class SharadarClient:
    """Direct Sharadar API facade with safe retries and injectable transport."""

    def __init__(
        self,
        api_key: str,
        *,
        session: SessionLike | None = None,
        base_url: str = API_BASE_URL,
        timeout: tuple[float, float] = (10.0, 60.0),
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] = random.uniform,
        user_agent: str = USER_AGENT,
    ) -> None:
        if not isinstance(api_key, str) or not api_key.strip():
            raise SharadarConfigurationError("Sharadar API key is empty")
        parsed_base = urlparse(base_url)
        try:
            parsed_port = parsed_base.port
        except ValueError:
            raise SharadarConfigurationError(
                "Sharadar base URL has an invalid port"
            ) from None
        if (
            parsed_base.scheme != "https"
            or parsed_base.hostname != "api.sharadar.com"
            or parsed_port not in (None, 443)
            or parsed_base.username is not None
            or parsed_base.password is not None
            or parsed_base.params
            or parsed_base.query
            or parsed_base.fragment
        ):
            raise SharadarConfigurationError(
                "Sharadar base URL must use the first-party HTTPS API origin"
            )
        if (
            not isinstance(timeout, tuple)
            or len(timeout) != 2
            or any(
                not isinstance(item, (int, float))
                or isinstance(item, bool)
                or not math.isfinite(item)
                or item <= 0
                for item in timeout
            )
        ):
            raise SharadarConfigurationError("timeout must contain positive connect/read values")

        self._api_key = api_key.strip()
        self._session: SessionLike = session or requests.Session()
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._retry = retry_policy or RetryPolicy()
        self._sleep = sleep
        self._random_uniform = random_uniform
        self._headers = {
            "x-api-key": self._api_key,
            "User-Agent": user_agent,
        }

    def __repr__(self) -> str:
        return (
            f"SharadarClient(base_url={self._base_url!r}, "
            f"timeout={self._timeout!r}, api_key='<redacted>')"
        )

    def query(
        self,
        table: object | QuerySpec,
        *,
        params: Mapping[str, object] | None = None,
        output_format: object | None = None,
    ) -> Any:
        """Query with a :class:`QuerySpec` or the exploratory raw-params lane.

        New ingestion code should pass ``QuerySpec``.  ``params`` remains a
        deliberately visible compatibility/experimental escape hatch for newly
        documented upstream fields; it is still auth-safe and deterministic.
        """

        if isinstance(table, QuerySpec):
            if params is not None or output_format is not None:
                raise SharadarConfigurationError(
                    "QuerySpec cannot be combined with raw params or format overrides"
                )
            normalized_table = table.table
            normalized_format = table.output_format
            query_params = table.parameters()
        else:
            normalized_table = _normalize_table(table)
            normalized_format = _normalize_output_format(output_format or "json")
            query_params = _safe_params(params)
            query_params["format"] = normalized_format
        response = self._request(
            f"/data/{normalized_table}",
            params=query_params,
            operation=f"query table {normalized_table}",
        )
        if normalized_format == "json":
            return self._decode_json(response, operation=f"query table {normalized_table}")
        return self._decode_csv(response, operation=f"query table {normalized_table}")

    def query_spec(self, spec: QuerySpec) -> Any:
        """Explicit typed-query entry point for ingestion callers."""

        if not isinstance(spec, QuerySpec):
            raise SharadarConfigurationError("query_spec requires a QuerySpec")
        return self.query(spec)

    def query_raw(
        self,
        table: object,
        *,
        params: Mapping[str, object] | None = None,
        output_format: object = "json",
    ) -> Any:
        """Explicit exploratory escape hatch for unmodeled upstream filters."""

        return self.query(table, params=params, output_format=output_format)

    def query_json(
        self,
        table: object | QuerySpec,
        *,
        params: Mapping[str, object] | None = None,
    ) -> Any:
        if isinstance(table, QuerySpec):
            if table.output_format != "json" or params is not None:
                raise SharadarConfigurationError(
                    "query_json requires a JSON QuerySpec without raw params"
                )
            return self.query(table)
        return self.query_raw(table, params=params, output_format="json")

    def query_csv(
        self,
        table: object | QuerySpec,
        *,
        params: Mapping[str, object] | None = None,
    ) -> str:
        if isinstance(table, QuerySpec):
            if table.output_format != "csv" or params is not None:
                raise SharadarConfigurationError(
                    "query_csv requires a CSV QuerySpec without raw params"
                )
            return self.query(table)
        return self.query_raw(table, params=params, output_format="csv")

    def iter_json_rows(
        self,
        table: object,
        *,
        params: Mapping[str, object] | None = None,
        page_size: int = 10_000,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> Iterator[Mapping[str, Any]]:
        """Yield a bounded REST slice with O(page_size) memory.

        Full-table history belongs on the bulk path. This iterator is for an
        already bounded ticker/date/lastupdated partition and owns ``limit`` and
        ``skip`` so callers cannot accidentally create overlapping pages.
        """

        if (
            not isinstance(page_size, int)
            or isinstance(page_size, bool)
            or not 1 <= page_size <= 10_000
        ):
            raise SharadarConfigurationError("page_size must be between 1 and 10000")
        if (
            not isinstance(max_pages, int)
            or isinstance(max_pages, bool)
            or max_pages < 1
        ):
            raise SharadarConfigurationError("max_pages must be a positive integer")

        base_params = _safe_params(params)
        controlled = {"limit", "skip", "offset", "format"}
        if any(str(key).lower() in controlled for key in base_params):
            raise SharadarConfigurationError(
                "iter_json_rows owns limit, skip, offset, and format"
            )

        page_number = 0
        full_page_fingerprints: set[str] = set()
        while True:
            page_params = dict(base_params)
            page_params.update({"limit": page_size, "skip": page_number * page_size})
            payload = self.query_raw(table, params=page_params, output_format="json")
            if not isinstance(payload, Mapping):
                raise SharadarDecodeError("paginated response was not a JSON object")
            rows = payload.get("data")
            if not isinstance(rows, list):
                raise SharadarDecodeError("paginated response data was not a list")
            if any(not isinstance(row, Mapping) for row in rows):
                raise SharadarDecodeError("paginated response contained a non-object row")

            if len(rows) == page_size:
                fingerprint = hashlib.sha256(
                    json.dumps(
                        rows,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        default=str,
                    ).encode("utf-8")
                ).hexdigest()
                if fingerprint in full_page_fingerprints:
                    raise SharadarResponseError(
                        "pagination repeated a full page; refusing an infinite loop"
                    )
                full_page_fingerprints.add(fingerprint)

            yield from rows
            page_number += 1
            if len(rows) < page_size:
                return
            if page_number >= max_pages:
                raise SharadarResponseError(
                    "pagination reached max_pages before a terminal page"
                )

    def schema_index(self) -> Any:
        """Fetch the public schema index (no API key is required upstream).

        This public request carries only the user-agent.  Redirects remain
        disabled, matching the authenticated request policy.
        """

        response = self._request(
            "/schema", operation="fetch schema index", authenticated=False
        )
        return self._decode_json(response, operation="fetch schema index")

    def schema(self, table: object, *, dialect: object = "postgres") -> str:
        normalized_table = _normalize_table(table)
        normalized_dialect = _plain_value(dialect).strip().lower()
        if normalized_dialect not in _SCHEMA_FORMATS:
            raise SharadarConfigurationError(
                "schema format must be postgres, sqlite, or mysql"
            )
        response = self._request(
            f"/schema/{normalized_table}",
            params={"format": normalized_dialect},
            operation=f"fetch schema for {normalized_table}",
            authenticated=False,
        )
        return self._decode_text(
            response, operation=f"fetch schema for {normalized_table}"
        )

    # A descriptive alias is useful to callers and avoids confusion with a
    # local Python schema object.
    table_schema = schema

    def bulk_status(self, table: object) -> Mapping[str, Any]:
        normalized_table = _normalize_table(table)
        response = self._request(
            f"/data/{normalized_table}",
            params={"status": "True"},
            operation=f"fetch bulk status for {normalized_table}",
        )
        payload = self._decode_json(
            response, operation=f"fetch bulk status for {normalized_table}"
        )
        if not isinstance(payload, Mapping):
            raise SharadarDecodeError(
                f"bulk status for {normalized_table} was not a JSON object"
            )
        return payload

    def bulk_redirect(
        self, table: object, history: object = "full"
    ) -> BulkRedirect:
        """Extract (but never follow) a time-limited bulk download location."""

        normalized_table = _normalize_table(table)
        normalized_history = _normalize_history(history)
        response = self._request(
            f"/data/{normalized_table}",
            params={"years": normalized_history},
            operation=f"request bulk redirect for {normalized_table}",
            expected_statuses=frozenset({302}),
        )
        location = response.headers.get("Location") or response.headers.get("location")
        if not location:
            raise SharadarRedirectError(
                f"bulk redirect for {normalized_table} omitted Location",
                status_code=response.status_code,
            )
        parsed = urlparse(location)
        try:
            redirect_port = parsed.port
        except ValueError:
            raise SharadarRedirectError(
                f"bulk redirect for {normalized_table} had an invalid port",
                status_code=response.status_code,
            ) from None
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or redirect_port not in (None, 443)
            or parsed.username is not None
            or parsed.password is not None
            or bool(parsed.fragment)
        ):
            raise SharadarRedirectError(
                f"bulk redirect for {normalized_table} was not a safe absolute HTTPS URL",
                status_code=response.status_code,
            )
        return BulkRedirect(
            location=location,
            table=normalized_table,
            history=normalized_history,
        )

    def _request(
        self,
        path: str,
        *,
        params: Mapping[str, object] | None = None,
        operation: str,
        expected_statuses: frozenset[int] | None = None,
        authenticated: bool = True,
    ) -> ResponseLike:
        expected = expected_statuses or frozenset(range(200, 300))
        url = f"{self._base_url}{path}"
        last_retry_status: int | None = None

        for attempt in range(1, self._retry.attempts + 1):
            try:
                response = self._session.request(
                    "GET",
                    url,
                    params=dict(params or {}),
                    headers=dict(self._headers) if authenticated else {
                        "User-Agent": self._headers["User-Agent"]
                    },
                    timeout=self._timeout,
                    allow_redirects=False,
                )
            except (requests.Timeout, requests.ConnectionError):
                if attempt == self._retry.attempts:
                    raise SharadarTransportError(
                        f"{operation} failed after {attempt} attempts",
                        retryable=True,
                    ) from None
                self._sleep(self._retry_delay(attempt, None))
                continue
            except requests.RequestException:
                raise SharadarTransportError(f"{operation} transport failed") from None

            status = int(response.status_code)
            if status in expected:
                return response
            if status in self._retry.retryable_statuses:
                last_retry_status = status
                if attempt < self._retry.attempts:
                    self._sleep(self._retry_delay(attempt, response.headers))
                    continue
            self._raise_status(operation, status)

        # The loop always returns or raises.  This protects type checkers and
        # future retry-policy edits without retaining the response object.
        if last_retry_status == 429:
            raise SharadarRateLimitError(
                f"{operation} remained rate limited",
                status_code=429,
                retryable=True,
            )
        raise SharadarTransportError(
            f"{operation} exhausted retries",
            status_code=last_retry_status,
            retryable=True,
        )

    def _raise_status(self, operation: str, status: int) -> None:
        if status == 401:
            raise SharadarAuthenticationError(
                f"{operation} was rejected by authentication",
                status_code=status,
            )
        if status == 403:
            raise SharadarEntitlementError(
                f"{operation} was rejected by entitlement",
                status_code=status,
            )
        if status == 429:
            raise SharadarRateLimitError(
                f"{operation} remained rate limited",
                status_code=status,
                retryable=True,
            )
        raise SharadarResponseError(
            f"{operation} returned unexpected HTTP status {status}",
            status_code=status,
            retryable=status in self._retry.retryable_statuses,
        )

    def _retry_delay(
        self, attempt: int, headers: Mapping[str, str] | None
    ) -> float:
        retry_after = self._parse_retry_after(headers)
        if retry_after is not None:
            return min(retry_after, self._retry.max_delay_seconds)
        ceiling = min(
            self._retry.max_delay_seconds,
            self._retry.base_delay_seconds * (2 ** (attempt - 1)),
        )
        return self._random_uniform(0.0, ceiling)

    @staticmethod
    def _parse_retry_after(headers: Mapping[str, str] | None) -> float | None:
        if not headers:
            return None
        raw = next(
            (
                value
                for key, value in headers.items()
                if str(key).lower() == "retry-after"
            ),
            None,
        )
        if not raw:
            return None
        try:
            seconds = float(raw)
            return max(0.0, seconds) if math.isfinite(seconds) else None
        except (TypeError, ValueError):
            try:
                retry_at = email.utils.parsedate_to_datetime(raw)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())

    @staticmethod
    def _decode_json(response: ResponseLike, *, operation: str) -> Any:
        content_type = SharadarClient._header(response.headers, "content-type")
        if content_type and "json" not in content_type.lower():
            raise SharadarDecodeError(
                f"{operation} returned a non-JSON content type"
            )
        try:
            preview = response.text.lstrip().lower()
        except (AttributeError, UnicodeError):
            preview = ""
        if preview.startswith(("<!doctype html", "<html")):
            raise SharadarDecodeError(f"{operation} returned HTML instead of JSON")
        try:
            return response.json()
        except (TypeError, ValueError):
            # Never include response.text: an upstream error may echo request data.
            raise SharadarDecodeError(f"{operation} returned invalid JSON") from None

    @staticmethod
    def _decode_text(response: ResponseLike, *, operation: str) -> str:
        try:
            text = response.text
        except (AttributeError, UnicodeError):
            raise SharadarDecodeError(f"{operation} returned invalid text") from None
        if not isinstance(text, str):
            raise SharadarDecodeError(f"{operation} returned invalid text")
        return text

    @staticmethod
    def _decode_csv(response: ResponseLike, *, operation: str) -> str:
        content_type = SharadarClient._header(response.headers, "content-type")
        if content_type:
            normalized = content_type.lower()
            allowed = (
                "text/csv",
                "application/csv",
                "text/plain",
                "application/octet-stream",
            )
            if not any(item in normalized for item in allowed):
                raise SharadarDecodeError(
                    f"{operation} returned a non-CSV content type"
                )
        text = SharadarClient._decode_text(response, operation=operation)
        preview = text.lstrip().lower()
        if preview.startswith(("<!doctype html", "<html")):
            raise SharadarDecodeError(f"{operation} returned HTML instead of CSV")
        return text

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str | None:
        expected = name.lower()
        return next(
            (
                str(value)
                for key, value in headers.items()
                if str(key).lower() == expected
            ),
            None,
        )
