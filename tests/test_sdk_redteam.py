"""High-value adversarial SDK tests that remain fully offline."""

from __future__ import annotations

import io
import math
import stat
import zipfile
from collections import deque
from pathlib import Path
from typing import Any

import pytest

from sharadar_pipeline.bulk import (
    BulkDownloader,
    BulkDownloadError,
    BulkIntegrityError,
)
from sharadar_pipeline.catalog import SchemaDialect, SharadarTable
from sharadar_pipeline.client import QueryFilter, RetryPolicy, SharadarClient
from sharadar_pipeline.errors import (
    SharadarConfigurationError,
    SharadarDecodeError,
    SharadarRedirectError,
    SharadarResponseError,
    redact_sensitive,
)
from sharadar_pipeline.routes import (
    Deployment,
    RouteError,
    SharadarRoute,
    require_route_io,
)


class _ClientResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        text: str = "CREATE TABLE synthetic (ticker text);",
        payload: object | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.payload = {"data": []} if payload is None else payload

    def json(self) -> object:
        return self.payload


class _ClientSession:
    def __init__(self, *responses: _ClientResponse) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.responses = deque(responses or [_ClientResponse()])

    def request(self, method: str, url: str, **kwargs: Any) -> _ClientResponse:
        self.calls.append((method, url, kwargs))
        return self.responses.popleft()


@pytest.mark.parametrize("table", list(SharadarTable))
def test_every_schema_route_is_canonical_and_credential_free(
    table: SharadarTable,
) -> None:
    session = _ClientSession()
    client = SharadarClient("SDK_TEST_SECRET_DO_NOT_LOG", session=session)

    client.schema(table, dialect=SchemaDialect.POSTGRES)

    _method, url, kwargs = session.calls[0]
    assert url == f"https://api.sharadar.com/v1.0/schema/{table.value}"
    assert kwargs["params"] == {"format": "postgres"}
    assert kwargs["allow_redirects"] is False
    assert {key.lower() for key in kwargs["headers"]} == {"user-agent"}


class _BulkClient:
    def __init__(
        self, payload_size: int, *, status: dict[str, object] | None = None
    ) -> None:
        self.payload_size = payload_size
        self.status_calls = 0
        self.redirect_calls = 0
        self.status = status or {
            "table": "fundamentals",
            "name": "fundamentals.csv.zip",
            "size": self.payload_size,
        }

    def bulk_status(self, _table: object) -> object:
        self.status_calls += 1
        return self.status

    def bulk_redirect(self, _table: object, _years: object) -> object:
        self.redirect_calls += 1
        return "https://objects.example.invalid/bulk.zip?signature=never-log"


class _BulkResponse:
    status_code = 200

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.headers = {"Content-Length": str(len(payload))}

    def iter_content(self, *, chunk_size: int):
        for offset in range(0, len(self.payload), chunk_size):
            yield self.payload[offset : offset + chunk_size]

    def close(self) -> None:
        return None


class _BulkSession:
    def __init__(
        self, payload: bytes, *, headers: dict[str, str] | None = None
    ) -> None:
        self.headers = headers or {}
        self.responses = deque([_BulkResponse(payload)])
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, url: str, **kwargs: object) -> _BulkResponse:
        self.calls.append((url, kwargs))
        return self.responses.popleft()


def _zip_with_member(member: str, *, symlink: bool = False) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        if symlink:
            info = zipfile.ZipInfo(member)
            info.create_system = 3
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(info, "target.csv")
        else:
            archive.writestr(member, "ticker,date\nAAPL,2026-08-31\n")
    return output.getvalue()


@pytest.mark.parametrize(
    "session_headers",
    [
        {"Authorization": "Bearer SDK_TEST_SECRET_DO_NOT_LOG"},
        {"Proxy-Authorization": "Basic SDK_TEST_SECRET_DO_NOT_LOG"},
        {"Api-Key": "SDK_TEST_SECRET_DO_NOT_LOG"},
        {"Vendor-API-Key": "SDK_TEST_SECRET_DO_NOT_LOG"},
    ],
)
def test_signed_object_session_rejects_all_credential_header_variants(
    tmp_path, session_headers: dict[str, str]
) -> None:
    payload = _zip_with_member("fundamentals.csv")
    session = _BulkSession(payload, headers=session_headers)
    downloader = BulkDownloader(
        _BulkClient(len(payload)), download_session=session, chunk_size=32
    )

    with pytest.raises(BulkDownloadError, match="contains credentials"):
        downloader.download("fundamentals", "full", tmp_path)

    assert session.calls == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "member",
    [
        r"..\escape.csv",
        r"C:\escape.csv",
        "/absolute.csv",
    ],
)
def test_zip_rejects_windows_and_absolute_member_traversal(
    tmp_path, member: str
) -> None:
    payload = _zip_with_member(member)
    downloader = BulkDownloader(
        _BulkClient(len(payload)),
        download_session=_BulkSession(payload),
        chunk_size=32,
    )

    with pytest.raises(BulkIntegrityError, match="path traversal"):
        downloader.download("fundamentals", "full", tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_zip_rejects_symbolic_link_member(tmp_path) -> None:
    payload = _zip_with_member("fundamentals.csv", symlink=True)
    downloader = BulkDownloader(
        _BulkClient(len(payload)),
        download_session=_BulkSession(payload),
        chunk_size=32,
    )

    with pytest.raises(BulkIntegrityError, match="symbolic-link"):
        downloader.download("fundamentals", "full", tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_forged_route_cannot_authorize_dev_write_into_prod() -> None:
    forged = SharadarRoute(
        deployment=Deployment.DEV,
        database_name="SHARADAR_PROD",
        artifact_root=Path("/Volumes/Pentagon_Quant/sharadar/prod"),
        write_confirmation="FORGED",
        live_write_authorized=True,
    )

    with pytest.raises(RouteError, match="route_for"):
        require_route_io(
            forged,
            database_name="SHARADAR_PROD",
            write=True,
            confirmation="FORGED",
        )


@pytest.mark.parametrize(
    ("attribute", "state"),
    [
        ("cookies", {"session": "SDK_TEST_SECRET_DO_NOT_LOG"}),
        ("auth", ("user", "SDK_TEST_SECRET_DO_NOT_LOG")),
        ("hooks", {"response": [lambda response: response]}),
        ("proxies", {"https": "https://proxy.example.invalid"}),
    ],
)
def test_signed_object_session_rejects_hidden_credential_state_before_network(
    tmp_path, attribute: str, state: object
) -> None:
    payload = _zip_with_member("fundamentals.csv")
    session = _BulkSession(payload)
    setattr(session, attribute, state)
    client = _BulkClient(len(payload))
    downloader = BulkDownloader(
        client, download_session=session, chunk_size=32
    )

    with pytest.raises(BulkDownloadError) as caught:
        downloader.download("fundamentals", "full", tmp_path)

    assert "SDK_TEST_SECRET_DO_NOT_LOG" not in str(caught.value)
    assert client.status_calls == 0
    assert client.redirect_calls == 0
    assert session.calls == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "status",
    [
        {
            "table": "fundamentals",
            "name": "api_key=SDK_TEST_SECRET_DO_NOT_LOG.zip",
        },
        {
            "table": "fundamentals",
            "name": "fundamentals.csv.zip",
            "modified": "token=SDK_TEST_SECRET_DO_NOT_LOG",
        },
    ],
)
def test_untrusted_bulk_status_metadata_never_persists_a_secret(
    tmp_path, status: dict[str, object]
) -> None:
    sentinel = "SDK_TEST_SECRET_DO_NOT_LOG"
    payload = _zip_with_member("fundamentals.csv")
    status = {**status, "size": len(payload)}
    downloader = BulkDownloader(
        _BulkClient(len(payload), status=status),
        download_session=_BulkSession(payload),
        chunk_size=32,
    )

    try:
        downloader.download("fundamentals", "full", tmp_path)
    except BulkDownloadError as exc:
        assert sentinel not in str(exc)
        assert sentinel not in repr(exc)

    for path in tmp_path.rglob("*"):
        assert sentinel not in path.name
        if path.is_file():
            assert sentinel not in path.read_text(encoding="utf-8", errors="ignore")


class _RepeatingPageSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> _ClientResponse:
        self.calls.append((method, url, kwargs))
        return _ClientResponse(payload={"data": [{"ticker": "AAPL"}]})


def test_repeated_full_pagination_page_fails_before_max_page_budget() -> None:
    session = _RepeatingPageSession()
    client = SharadarClient(
        "SDK_TEST_SECRET_DO_NOT_LOG",
        session=session,
        retry_policy=RetryPolicy(attempts=1),
    )

    with pytest.raises(SharadarResponseError, match="repeat|progress|pagination"):
        list(
            client.iter_json_rows(
                "daily", page_size=1, max_pages=100
            )
        )

    assert len(session.calls) <= 3


@pytest.mark.parametrize(
    "params",
    [
        {"from": "2023-09-31"},
        {"from": "2026-09-01", "to": "2026-08-31"},
        {"revenue": math.nan},
        {"ticker": ["AAPL", "AAPL"]},
        {"from": "2026-08-01", "date.gte": "2026-08-01"},
        {"skip": 0, "offset": 0},
        {"date.ne": "2026-08-01"},
    ],
)
def test_invalid_or_conflicting_typed_query_fails_before_network(
    params: dict[str, object]
) -> None:
    session = _ClientSession()
    client = SharadarClient("SDK_TEST_SECRET_DO_NOT_LOG", session=session)

    with pytest.raises(SharadarConfigurationError):
        client.query_json("fundamentals", params=params)

    assert session.calls == []


def _zip_with_members(
    members: list[tuple[str, bytes]], *, compression: int = zipfile.ZIP_STORED
) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression) as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return output.getvalue()


@pytest.mark.parametrize(
    "payload",
    [
        _zip_with_members(
            [
                ("fundamentals.csv", b"ticker,date\nAAPL,2026-08-31\n"),
                ("extra.csv", b"ticker,date\nAAPL,2026-08-31\n"),
            ]
        ),
        _zip_with_members(
            [("nested/fundamentals.csv", b"ticker,date\nAAPL,2026-08-31\n")]
        ),
        _zip_with_members(
            [
                ("fundamentals.csv", b"ticker,date\nAAPL,2026-08-31\n"),
                ("FUNDAMENTALS.CSV", b"ticker,date\nAAPL,2026-08-31\n"),
            ]
        ),
        _zip_with_members(
            [("fundamentals.csv", b"ticker,date\n" + b"0" * (5 * 1024 * 1024))],
            compression=zipfile.ZIP_DEFLATED,
        ),
    ],
    ids=["multiple-csv", "nested-csv", "case-collision", "zip-bomb"],
)
def test_ambiguous_schema_invalid_or_resource_hostile_zip_is_rejected(
    tmp_path, payload: bytes
) -> None:
    downloader = BulkDownloader(
        _BulkClient(len(payload)),
        download_session=_BulkSession(payload),
        chunk_size=64 * 1024,
    )

    with pytest.raises(BulkIntegrityError):
        downloader.download("fundamentals", "full", tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_bulk_header_must_match_explicit_schema_policy(tmp_path) -> None:
    payload = _zip_with_members(
        [("fundamentals.csv", b"ticker,date,wrong\nAAPL,2026-08-31,1\n")]
    )
    downloader = BulkDownloader(
        _BulkClient(len(payload)),
        download_session=_BulkSession(payload),
        chunk_size=64 * 1024,
        expected_headers={"fundamentals": ("ticker", "date", "revenue")},
    )

    with pytest.raises(BulkIntegrityError, match="schema"):
        downloader.download("fundamentals", "full", tmp_path)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "location",
    [
        "https://user:password@objects.example.invalid/file.zip",
        "https://objects.example.invalid/file.zip#signed-fragment",
    ],
)
def test_client_rejects_credentialed_or_fragmented_bulk_redirect(
    location: str,
) -> None:
    session = _ClientSession(
        _ClientResponse(status_code=302, headers={"Location": location})
    )

    with pytest.raises(SharadarRedirectError):
        SharadarClient(
            "SDK_TEST_SECRET_DO_NOT_LOG", session=session
        ).bulk_redirect("fundamentals", "full")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"attempts": True},
        {"attempts": 1.5},
        {"base_delay_seconds": math.nan},
        {"base_delay_seconds": math.inf},
        {"max_delay_seconds": math.nan},
        {"max_delay_seconds": math.inf},
    ],
)
def test_retry_policy_rejects_bool_fractional_and_nonfinite_values(
    kwargs: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field", ["date", "lastupdated", "calendardate", "reportperiod"]
)
def test_typed_date_filter_rejects_nonexistent_calendar_date(field: str) -> None:
    with pytest.raises(SharadarConfigurationError, match="date|calendar"):
        QueryFilter(field, "gte", "2023-09-31")


def test_typed_filter_multi_value_rejects_duplicates() -> None:
    with pytest.raises(SharadarConfigurationError, match="duplicate"):
        QueryFilter("dimension", "eq", ("ARQ", "ARQ"))


@pytest.mark.parametrize("ratio", [math.nan, math.inf])
def test_zip_bomb_ratio_guard_cannot_be_disabled_by_nonfinite_config(
    ratio: float,
) -> None:
    payload = _zip_with_member("fundamentals.csv")
    with pytest.raises(ValueError, match="compression"):
        BulkDownloader(
            _BulkClient(len(payload)),
            download_session=_BulkSession(payload),
            max_compression_ratio=ratio,
        )


@pytest.mark.parametrize("output_format", ["json", "csv"])
def test_successful_html_response_is_never_accepted_as_market_data(
    output_format: str,
) -> None:
    session = _ClientSession(
        _ClientResponse(
            headers={"Content-Type": "text/html; charset=utf-8"},
            text="<html>login</html>",
            payload={"data": []},
        )
    )
    client = SharadarClient("SDK_TEST_SECRET_DO_NOT_LOG", session=session)

    with pytest.raises(SharadarDecodeError, match="content type|HTML"):
        client.query("fundamentals", output_format=output_format)


def test_bearer_and_cookie_values_are_redacted() -> None:
    sentinel = "SDK_TEST_SECRET_DO_NOT_LOG"
    rendered = redact_sensitive(
        f"Authorization: Bearer {sentinel}; Cookie: session={sentinel}"
    )

    assert sentinel not in rendered
