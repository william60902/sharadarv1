from __future__ import annotations

import math
from collections import deque
from datetime import date
from typing import Any

import pytest
import requests

from sharadar_pipeline.catalog import (
    HistoryWindow,
    OutputFormat,
    SchemaDialect,
    SharadarTable,
)
from sharadar_pipeline.client import (
    BulkRedirect,
    FilterOperator,
    QueryFilter,
    QuerySpec,
    RetryPolicy,
    SharadarClient,
    SortDirection,
    SortSpec,
)
from sharadar_pipeline.errors import (
    SharadarAuthenticationError,
    SharadarConfigurationError,
    SharadarDecodeError,
    SharadarEntitlementError,
    SharadarRateLimitError,
    SharadarRedirectError,
    SharadarResponseError,
    SharadarTransportError,
    redact_sensitive,
)

_KEY = "paid-secret-key-do-not-print"
_SIGNED_URL = (
    "https://download.example.test/full.zip?X-Amz-Signature=signature-secret"
    "&X-Amz-Credential=credential-secret"
)


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        *,
        json_data: Any = None,
        text: str = "",
        headers: dict[str, str] | None = None,
        json_error: Exception | None = None,
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self.headers = headers or {}
        self._json_error = json_error

    def json(self) -> Any:
        if self._json_error is not None:
            raise self._json_error
        return self._json_data


class FakeSession:
    def __init__(self, *outcomes: object) -> None:
        self.outcomes = deque(outcomes)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append((method, url, kwargs))
        if not self.outcomes:
            raise AssertionError("unexpected HTTP request")
        outcome = self.outcomes.popleft()
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, FakeResponse)
        return outcome


def make_client(
    session: FakeSession,
    *,
    retry_policy: RetryPolicy | None = None,
    sleeps: list[float] | None = None,
) -> SharadarClient:
    captured_sleeps = sleeps if sleeps is not None else []
    return SharadarClient(
        _KEY,
        session=session,
        retry_policy=retry_policy,
        sleep=captured_sleeps.append,
        random_uniform=lambda _low, high: high,
    )


def test_query_json_normalizes_alias_and_encodes_sequences() -> None:
    session = FakeSession(FakeResponse(json_data={"data": [{"ticker": "AAPL"}]}))
    client = make_client(session)

    result = client.query(
        "SF1",
        output_format=OutputFormat.JSON,
        params={
            "ticker": ["AAPL", "MSFT"],
            "dimension": ("ARQ", "ART"),
            "lastupdated.gte": "2026-08-01",
            "unused": None,
        },
    )

    assert result == {"data": [{"ticker": "AAPL"}]}
    method, url, kwargs = session.calls[0]
    assert method == "GET"
    assert url == "https://api.sharadar.com/v1.0/data/fundamentals"
    assert kwargs["params"] == {
        "ticker": "AAPL,MSFT",
        "dimension": "ARQ,ART",
        "lastupdated.gte": "2026-08-01",
        "format": "json",
    }
    assert kwargs["headers"] == {
        "x-api-key": _KEY,
        "User-Agent": "Medina-SharadarV1/0.1",
    }
    assert kwargs["allow_redirects"] is False
    assert kwargs["timeout"] == (10.0, 60.0)
    assert "api_key" not in kwargs["params"]
    assert _KEY not in url


@pytest.mark.parametrize("table", list(SharadarTable))
def test_every_documented_table_uses_one_canonical_data_route(
    table: SharadarTable,
) -> None:
    session = FakeSession(FakeResponse(json_data={"data": []}))

    make_client(session).query_json(table)

    assert session.calls[0][1] == (
        f"https://api.sharadar.com/v1.0/data/{table.value}"
    )


def test_query_csv_returns_text_and_overrides_format_param() -> None:
    session = FakeSession(FakeResponse(text="ticker,date\nAAPL,2026-08-28\n"))
    client = make_client(session)

    value = client.query_csv(
        SharadarTable.STOCKS,
        params={"format": "json", "fields": ("ticker", "date")},
    )

    assert value.startswith("ticker,date")
    assert session.calls[0][2]["params"]["format"] == "csv"
    assert set(session.calls[0][2]["params"]["fields"].split(",")) == {
        "ticker",
        "date",
    }


def test_typed_query_spec_encodes_validated_parameters_deterministically() -> None:
    session = FakeSession(FakeResponse(json_data={"data": []}))
    spec = QuerySpec(
        table="SF1",
        output_format=OutputFormat.JSON,
        tickers=["BRK-B", "AAPL"],
        from_date=date(2024, 2, 29),
        to_date="2026-08-31",
        fields=["ticker", "date", "revenue"],
        filters=[
            QueryFilter("dimension", FilterOperator.EQ, ("ARQ", "ART")),
            QueryFilter("lastupdated", FilterOperator.GTE, "2026-08-01"),
        ],
        sort=SortSpec("date", SortDirection.ASC),
        limit=500,
        skip=10,
    )

    assert make_client(session).query_spec(spec) == {"data": []}
    assert session.calls[0][1].endswith("/data/fundamentals")
    assert session.calls[0][2]["params"] == {
        "format": "json",
        "ticker": "BRK-B,AAPL",
        "from": "2024-02-29",
        "to": "2026-08-31",
        "fields": "ticker,date,revenue",
        "dimension": "ARQ,ART",
        "lastupdated.gte": "2026-08-01",
        "sort": "date.asc",
        "limit": 500,
        "skip": 10,
    }


@pytest.mark.parametrize(
    "kwargs, pattern",
    [
        ({"from_date": "2023-09-31"}, "real calendar"),
        ({"from_date": "2026-8-01"}, "YYYY-MM-DD"),
        (
            {"from_date": "2026-08-02", "to_date": "2026-08-01"},
            "cannot be after",
        ),
        ({"limit": True}, "limit"),
        ({"limit": 1.5}, "limit"),
        ({"limit": 0}, "limit"),
        ({"skip": True}, "skip"),
        ({"skip": -1}, "skip"),
        ({"tickers": {"AAPL", "MSFT"}}, "ordered"),
        ({"fields": {"ticker", "date"}}, "ordered"),
        ({"tickers": ["AAPL", "AAPL"]}, "duplicates"),
        ({"fields": ["ticker", "ticker"]}, "duplicates"),
    ],
)
def test_query_spec_rejects_invalid_corner_values(
    kwargs: dict[str, Any], pattern: str
) -> None:
    with pytest.raises(SharadarConfigurationError, match=pattern):
        QuerySpec("fundamentals", **kwargs)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), None, True, []])
def test_query_filter_rejects_invalid_values(value: object) -> None:
    with pytest.raises(SharadarConfigurationError):
        QueryFilter("revenue", "gte", value)


@pytest.mark.parametrize("operator", ["ne", "in", "regex", ""])
def test_query_filter_rejects_undocumented_operators(operator: str) -> None:
    with pytest.raises(SharadarConfigurationError, match="operator"):
        QueryFilter("revenue", operator, 1)


@pytest.mark.parametrize(
    "field", ["date", "lastupdated", "calendardate", "reportperiod"]
)
def test_query_filter_validates_real_calendar_dates(field: str) -> None:
    with pytest.raises(SharadarConfigurationError, match="real calendar"):
        QueryFilter(field, "gte", "2023-09-31")


def test_query_filter_rejects_duplicate_ordered_multi_values() -> None:
    with pytest.raises(SharadarConfigurationError, match="duplicates"):
        QueryFilter("dimension", "eq", ("ARQ", "ARQ"))


def test_query_spec_rejects_duplicate_and_semantically_conflicting_filters() -> None:
    duplicate = QueryFilter("lastupdated", "gte", "2026-08-01")
    with pytest.raises(SharadarConfigurationError, match="duplicates"):
        QuerySpec("fundamentals", filters=[duplicate, duplicate])
    with pytest.raises(SharadarConfigurationError, match="conflicts"):
        QuerySpec(
            "fundamentals",
            from_date="2026-08-01",
            filters=[QueryFilter("date", "gte", "2026-08-01")],
        )


def test_typed_query_does_not_mutate_caller_sequences() -> None:
    tickers = ["AAPL", "MSFT"]
    fields = ["ticker", "date"]
    spec = QuerySpec("daily", tickers=tickers, fields=fields)
    tickers.append("IBM")
    fields.reverse()

    assert spec.tickers == ("AAPL", "MSFT")
    assert spec.fields == ("ticker", "date")


def test_raw_params_reject_unordered_sets() -> None:
    session = FakeSession()
    with pytest.raises(SharadarConfigurationError, match="ordered"):
        make_client(session).query_raw(
            "fundamentals", params={"fields": {"date", "ticker"}}
        )
    assert session.calls == []


@pytest.mark.parametrize(
    "forbidden", ["api_key", "apiKey", "x-api-key", "Authorization"]
)
def test_query_rejects_all_caller_supplied_auth_parameters(forbidden: str) -> None:
    session = FakeSession()
    client = make_client(session)

    with pytest.raises(SharadarConfigurationError, match="forbidden"):
        client.query_json("fundamentals", params={forbidden: "leak"})

    assert session.calls == []


def test_public_schema_index_and_table_do_not_send_api_key() -> None:
    session = FakeSession(
        FakeResponse(json_data={"tables": ["fundamentals"]}),
        FakeResponse(text="CREATE TABLE fundamentals (...);"),
    )
    client = make_client(session)

    assert client.schema_index() == {"tables": ["fundamentals"]}
    assert (
        client.table_schema("SF1", dialect=SchemaDialect.SQLITE)
        == "CREATE TABLE fundamentals (...);"
    )

    assert session.calls[0][1].endswith("/schema")
    assert session.calls[1][1].endswith("/schema/fundamentals")
    assert session.calls[1][2]["params"] == {"format": "sqlite"}
    for _method, _url, kwargs in session.calls:
        assert kwargs["headers"] == {"User-Agent": "Medina-SharadarV1/0.1"}
        assert "x-api-key" not in kwargs["headers"]


def test_schema_rejects_invalid_dialect_without_request() -> None:
    session = FakeSession()
    with pytest.raises(SharadarConfigurationError, match="schema format"):
        make_client(session).schema("fundamentals", dialect="oracle")
    assert session.calls == []


def test_bulk_status_returns_metadata_object() -> None:
    payload = {
        "table": "fundamentals",
        "name": "fundamentals.csv.zip",
        "size": 123,
        "lastModified": "2026-08-30T00:00:00Z",
    }
    session = FakeSession(FakeResponse(json_data=payload))

    assert make_client(session).bulk_status("SF1") == payload
    assert session.calls[0][2]["params"] == {"status": "True"}


def test_bulk_status_rejects_non_object_json() -> None:
    session = FakeSession(FakeResponse(json_data=["not", "metadata"]))
    with pytest.raises(SharadarDecodeError, match="not a JSON object"):
        make_client(session).bulk_status("fundamentals")


def test_bulk_redirect_is_not_followed_and_repr_is_redacted() -> None:
    session = FakeSession(
        FakeResponse(302, headers={"Location": _SIGNED_URL})
    )

    redirect = make_client(session).bulk_redirect(
        SharadarTable.FUNDAMENTALS, HistoryWindow.FULL
    )

    assert isinstance(redirect, BulkRedirect)
    assert redirect.location == _SIGNED_URL
    assert redirect.url == _SIGNED_URL
    assert redirect.table == "fundamentals"
    assert redirect.history == "full"
    assert _SIGNED_URL not in repr(redirect)
    assert "signature-secret" not in repr(redirect)
    assert "<redacted-url>" in str(redirect)
    _, api_url, kwargs = session.calls[0]
    assert api_url.endswith("/data/fundamentals")
    assert kwargs["params"] == {"years": "full"}
    assert kwargs["allow_redirects"] is False
    assert kwargs["headers"]["x-api-key"] == _KEY
    # There was exactly one first-party request; streaming is delegated to bulk.py.
    assert len(session.calls) == 1


@pytest.mark.parametrize(
    "response, pattern",
    [
        (FakeResponse(302), "omitted Location"),
        (FakeResponse(302, headers={"Location": "/relative.zip"}), "absolute HTTPS"),
        (
            FakeResponse(302, headers={"Location": "http://download.test/file.zip"}),
            "absolute HTTPS",
        ),
    ],
)
def test_bulk_redirect_rejects_missing_or_unsafe_location(
    response: FakeResponse, pattern: str
) -> None:
    with pytest.raises(SharadarRedirectError, match=pattern):
        make_client(FakeSession(response)).bulk_redirect("fundamentals", "5")


def test_regular_query_never_follows_an_unexpected_redirect() -> None:
    session = FakeSession(FakeResponse(302, headers={"Location": _SIGNED_URL}))
    with pytest.raises(SharadarResponseError) as captured:
        make_client(session).query_json("fundamentals")

    assert session.calls[0][2]["allow_redirects"] is False
    assert _SIGNED_URL not in str(captured.value)
    assert "signature-secret" not in repr(captured.value)


@pytest.mark.parametrize(
    "status, exception_type",
    [
        (401, SharadarAuthenticationError),
        (403, SharadarEntitlementError),
        (404, SharadarResponseError),
    ],
)
def test_http_errors_are_typed_and_never_include_body_or_key(
    status: int, exception_type: type[Exception]
) -> None:
    response = FakeResponse(
        status,
        text=f"echoed x-api-key={_KEY} signed={_SIGNED_URL}",
    )
    with pytest.raises(exception_type) as captured:
        make_client(FakeSession(response)).query_json("fundamentals")

    rendered = f"{captured.value!s} {captured.value!r}"
    assert _KEY not in rendered
    assert _SIGNED_URL not in rendered
    assert "signature-secret" not in rendered


def test_invalid_json_does_not_echo_sensitive_response_body() -> None:
    response = FakeResponse(
        json_error=ValueError("invalid"),
        text=f"api_key={_KEY}&redirect={_SIGNED_URL}",
    )
    with pytest.raises(SharadarDecodeError) as captured:
        make_client(FakeSession(response)).query_json("daily")

    rendered = f"{captured.value!s} {captured.value!r}"
    assert _KEY not in rendered
    assert "signature-secret" not in rendered


def test_retries_transient_status_with_exponential_full_jitter() -> None:
    sleeps: list[float] = []
    session = FakeSession(
        FakeResponse(500),
        FakeResponse(503),
        FakeResponse(json_data={"data": []}),
    )
    policy = RetryPolicy(attempts=3, base_delay_seconds=0.5, max_delay_seconds=5)

    assert make_client(
        session, retry_policy=policy, sleeps=sleeps
    ).query_json("fundamentals") == {"data": []}
    assert sleeps == [0.5, 1.0]
    assert len(session.calls) == 3


def test_retry_after_header_is_honored_and_capped() -> None:
    sleeps: list[float] = []
    session = FakeSession(
        FakeResponse(429, headers={"Retry-After": "99"}),
        FakeResponse(json_data={"data": []}),
    )
    policy = RetryPolicy(attempts=2, max_delay_seconds=7)

    make_client(session, retry_policy=policy, sleeps=sleeps).query_json("tickers")

    assert sleeps == [7]


def test_retry_after_header_lookup_is_fully_case_insensitive() -> None:
    sleeps: list[float] = []
    session = FakeSession(
        FakeResponse(429, headers={"rEtRy-AfTeR": "3"}),
        FakeResponse(json_data={"data": []}),
    )

    make_client(
        session,
        retry_policy=RetryPolicy(attempts=2, max_delay_seconds=7),
        sleeps=sleeps,
    ).query_json("tickers")

    assert sleeps == [3]


def test_rate_limit_after_last_attempt_is_typed() -> None:
    session = FakeSession(FakeResponse(429), FakeResponse(429))
    policy = RetryPolicy(attempts=2, base_delay_seconds=0)

    with pytest.raises(SharadarRateLimitError) as captured:
        make_client(session, retry_policy=policy).query_json("tickers")

    assert captured.value.status_code == 429
    assert captured.value.retryable is True


def test_timeout_retries_then_succeeds_without_leaking_exception_url() -> None:
    session = FakeSession(
        requests.Timeout(f"timeout {_SIGNED_URL}"),
        FakeResponse(json_data={"data": []}),
    )
    policy = RetryPolicy(attempts=2, base_delay_seconds=0)

    assert make_client(session, retry_policy=policy).query_json("daily") == {
        "data": []
    }


def test_transport_error_after_exhaustion_is_redacted() -> None:
    session = FakeSession(
        requests.ConnectionError(f"failed {_SIGNED_URL}?api_key={_KEY}"),
        requests.ConnectionError("still failed"),
    )
    policy = RetryPolicy(attempts=2, base_delay_seconds=0)

    with pytest.raises(SharadarTransportError) as captured:
        make_client(session, retry_policy=policy).query_json("daily")

    rendered = f"{captured.value!s} {captured.value!r}"
    assert _KEY not in rendered
    assert "signature-secret" not in rendered


@pytest.mark.parametrize("table", ["../fundamentals", "bad/table", "unknown"])
def test_invalid_or_unknown_table_is_rejected_before_network(table: str) -> None:
    session = FakeSession()
    with pytest.raises(SharadarConfigurationError):
        make_client(session).query_json(table)
    assert session.calls == []


def test_api_key_and_client_repr_are_redacted() -> None:
    session = FakeSession()
    client = make_client(session)

    assert _KEY not in repr(client)
    assert "<redacted>" in repr(client)
    with pytest.raises(SharadarConfigurationError):
        SharadarClient("", session=session)


@pytest.mark.parametrize(
    "unsafe_base",
    [
        "http://api.sharadar.com/v1.0",
        "https://evil.example.test/v1.0",
        "https://api.sharadar.com@evil.example.test/v1.0",
        "https://api.sharadar.com/v1.0?api_key=leak",
        "https://api.sharadar.com/v1.0#fragment",
    ],
)
def test_base_url_cannot_redirect_api_key_to_another_origin(
    unsafe_base: str,
) -> None:
    with pytest.raises(SharadarConfigurationError, match="first-party"):
        SharadarClient(_KEY, session=FakeSession(), base_url=unsafe_base)


def test_redact_sensitive_removes_keys_and_complete_urls() -> None:
    value = redact_sensitive(
        f"api_key={_KEY} x-api-key: {_KEY} location={_SIGNED_URL}"
    )
    assert _KEY not in value
    assert "signature-secret" not in value
    assert "<redacted-url>" in value


def test_redact_sensitive_removes_bearer_and_cookie_values() -> None:
    value = redact_sensitive(
        f"Authorization: Bearer {_KEY}; Cookie: session={_KEY}; other=visible"
    )
    assert _KEY not in value
    assert "Bearer <redacted>" not in value  # entire authorization value is hidden


@pytest.mark.parametrize(
    "kwargs",
    [
        {"attempts": 0},
        {"attempts": True},
        {"attempts": 1.5},
        {"base_delay_seconds": -1},
        {"max_delay_seconds": -1},
        {"base_delay_seconds": math.nan},
        {"base_delay_seconds": math.inf},
        {"max_delay_seconds": math.nan},
        {"max_delay_seconds": math.inf},
    ],
)
def test_retry_policy_rejects_invalid_values(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


@pytest.mark.parametrize("history", ["1", "all", "", object()])
def test_bulk_redirect_validates_history_before_network(history: object) -> None:
    session = FakeSession()
    with pytest.raises(SharadarConfigurationError):
        make_client(session).bulk_redirect("fundamentals", history)
    assert session.calls == []


@pytest.mark.parametrize(
    "location",
    [
        "https://user:secret@download.test/file.zip",
        "https://download.test/file.zip#signed-secret",
        "https://download.test:not-a-port/file.zip",
    ],
)
def test_bulk_redirect_rejects_credentialed_fragmented_or_invalid_port(
    location: str,
) -> None:
    session = FakeSession(FakeResponse(302, headers={"Location": location}))
    with pytest.raises(SharadarRedirectError) as captured:
        make_client(session).bulk_redirect("fundamentals", "full")
    assert "secret" not in str(captured.value)


@pytest.mark.parametrize("output_format", ["json", "csv"])
def test_html_success_response_is_rejected_by_content_type(
    output_format: str,
) -> None:
    session = FakeSession(
        FakeResponse(
            headers={"cOnTeNt-TyPe": "text/html; charset=utf-8"},
            text="<html>login</html>",
            json_data={"data": []},
        )
    )
    with pytest.raises(SharadarDecodeError, match="content type|HTML"):
        make_client(session).query("fundamentals", output_format=output_format)
