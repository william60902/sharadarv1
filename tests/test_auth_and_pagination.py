from __future__ import annotations

from collections import deque

import pytest

from sharadar_pipeline.auth import client_from_vault
from sharadar_pipeline.client import RetryPolicy, SharadarClient
from sharadar_pipeline.errors import (
    SharadarConfigurationError,
    SharadarDecodeError,
    SharadarResponseError,
)


class Response:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self.headers = {}
        self.payload = payload

    def json(self):
        return self.payload


class Session:
    def __init__(self, *payloads):
        self.payloads = deque(payloads)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return Response(self.payloads.popleft())


def make_client(session):
    return SharadarClient(
        "secret-not-printed",
        session=session,
        retry_policy=RetryPolicy(attempts=1),
    )


def test_vault_adapter_passes_secret_in_memory_only():
    calls = []

    def getter(name, *, env):
        calls.append((name, env))
        return "vault-secret"

    session = Session()
    client = client_from_vault(secret_getter=getter, session=session)

    assert calls == [("sharadar_api_key", "prod")]
    assert "vault-secret" not in repr(client)


def test_pagination_streams_exact_pages_and_terminates():
    session = Session(
        {"data": [{"id": 1}, {"id": 2}]},
        {"data": [{"id": 3}, {"id": 4}]},
        {"data": [{"id": 5}]},
    )

    rows = list(
        make_client(session).iter_json_rows(
            "fundamentals",
            params={"ticker": "PLTR", "lastupdated.gte": "2026-08-01"},
            page_size=2,
        )
    )

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]
    assert [call[2]["params"]["skip"] for call in session.calls] == [0, 2, 4]
    assert all(call[2]["params"]["limit"] == 2 for call in session.calls)


def test_pagination_exact_multiple_requests_terminal_empty_page():
    session = Session({"data": [{"id": 1}]}, {"data": []})
    assert list(
        make_client(session).iter_json_rows("daily", page_size=1)
    ) == [{"id": 1}]
    assert len(session.calls) == 2


@pytest.mark.parametrize(
    "params",
    [{"limit": 1}, {"skip": 1}, {"offset": 1}, {"format": "json"}],
)
def test_pagination_rejects_caller_controlled_page_parameters(params):
    with pytest.raises(SharadarConfigurationError, match="owns"):
        list(make_client(Session()).iter_json_rows("daily", params=params))


@pytest.mark.parametrize("page_size", [0, -1, 10_001, True, 1.2])
def test_pagination_rejects_invalid_page_size(page_size):
    with pytest.raises(SharadarConfigurationError, match="page_size"):
        list(
            make_client(Session()).iter_json_rows(
                "daily", page_size=page_size
            )
        )


@pytest.mark.parametrize(
    "payload",
    [[], {}, {"data": {}}, {"data": [1]}],
)
def test_pagination_rejects_malformed_payload(payload):
    with pytest.raises(SharadarDecodeError):
        list(make_client(Session(payload)).iter_json_rows("daily", page_size=2))


def test_pagination_cap_fails_loudly_instead_of_returning_partial_success():
    session = Session({"data": [{"id": 1}]})
    with pytest.raises(SharadarResponseError, match="max_pages"):
        list(
            make_client(session).iter_json_rows(
                "daily", page_size=1, max_pages=1
            )
        )


@pytest.mark.performance
def test_pagination_has_linear_request_count_and_page_bounded_memory_shape():
    page_size = 1000
    page_count = 100
    pages = [
        {"data": [{"id": page * page_size + offset} for offset in range(page_size)]}
        for page in range(page_count)
    ]
    pages.append({"data": []})
    session = Session(*pages)

    count = sum(
        1
        for _ in make_client(session).iter_json_rows(
            "fundamentals", page_size=page_size
        )
    )

    assert count == page_count * page_size
    assert len(session.calls) == page_count + 1
