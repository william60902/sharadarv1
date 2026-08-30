from __future__ import annotations

import hashlib
import io
import json
import math
import zipfile
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pytest

from sharadar_pipeline.bulk import (
    BulkDownloader,
    BulkDownloadError,
    BulkIntegrityError,
    UnsafeBulkRedirect,
)

SIGNED_URL = "https://objects.example.invalid/bulk.zip?signature=redacted"


def make_zip(
    rows: bytes = b"ticker,date,value\nAAPL,2026-08-31,1\n",
    *,
    member: str = "fundamentals.csv",
    compression: int = zipfile.ZIP_STORED,
) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression) as archive:
        archive.writestr(member, rows)
    return output.getvalue()


def make_zip_entries(
    entries: tuple[tuple[str, bytes], ...],
    *,
    compression: int = zipfile.ZIP_STORED,
) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression) as archive:
        for name, rows in entries:
            archive.writestr(name, rows)
    return output.getvalue()


class FakeResponse:
    def __init__(
        self,
        payload: bytes,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        oversized_chunk: bool = False,
        stream_error: Exception | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {"Content-Length": str(len(payload))}
        self.oversized_chunk = oversized_chunk
        self.stream_error = stream_error
        self.requested_chunk_sizes: list[int] = []
        self.yield_count = 0
        self.closed = False

    def iter_content(self, *, chunk_size: int):
        self.requested_chunk_sizes.append(chunk_size)
        if self.stream_error is not None:
            raise self.stream_error
        step = chunk_size + 1 if self.oversized_chunk else chunk_size
        for offset in range(0, len(self.payload), step):
            self.yield_count += 1
            yield self.payload[offset : offset + step]

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(
        self,
        *responses: FakeResponse,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.responses = list(responses)
        self.headers = headers or {}
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError("unexpected download request")
        return self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


class FailingSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        raise RuntimeError(f"failed to fetch secret URL {url}")


class FakeClient:
    def __init__(
        self,
        payload_size: int,
        *,
        status: dict[str, object] | None = None,
        redirect: object = SIGNED_URL,
    ) -> None:
        self.status = status or {
            "table": "fundamentals",
            "name": "fundamentals.csv.zip",
            "size": payload_size,
            "modified": "2026-08-31T00:00:00+00:00",
        }
        self.redirect = redirect
        self.status_calls: list[object] = []
        self.redirect_calls: list[tuple[object, object]] = []

    def bulk_status(self, table: object) -> dict[str, object]:
        self.status_calls.append(table)
        return self.status

    def bulk_redirect(self, table: object, years: object) -> object:
        self.redirect_calls.append((table, years))
        return self.redirect


def downloader(
    payload: bytes,
    *,
    response: FakeResponse | None = None,
    session: FakeSession | None = None,
    client: FakeClient | None = None,
    chunk_size: int = 17,
) -> tuple[BulkDownloader, FakeClient, FakeSession, FakeResponse]:
    exact_response = response or FakeResponse(payload)
    exact_session = session or FakeSession(exact_response)
    exact_client = client or FakeClient(len(payload))
    return (
        BulkDownloader(
            exact_client,
            download_session=exact_session,
            chunk_size=chunk_size,
        ),
        exact_client,
        exact_session,
        exact_response,
    )


class BulkDownloaderTests(TestCase):
    def test_happy_path_streams_and_returns_url_free_manifest(self) -> None:
        payload = make_zip()
        engine, client, session, response = downloader(payload)

        with self.subTest("download"), TemporaryDirectoryPath() as target:
            receipt = engine.download("SF1", "full", target)
            manifest = receipt.manifest()

            self.assertEqual(receipt.table, "fundamentals")
            self.assertEqual(receipt.history, "full")
            self.assertEqual(receipt.source_name, "fundamentals.full.csv.zip")
            self.assertEqual(receipt.received_bytes, len(payload))
            self.assertEqual(receipt.sha256, hashlib.sha256(payload).hexdigest())
            self.assertEqual(receipt.csv_members, ("fundamentals.csv",))
            self.assertEqual(receipt.csv_headers, (("ticker", "date", "value"),))
            self.assertTrue(receipt.artifact_path.is_file())
            self.assertTrue(receipt.manifest_path.is_file())
            self.assertEqual(json.loads(receipt.manifest_path.read_text()), manifest)
            self.assertNotIn("url", json.dumps(manifest).lower())
            self.assertNotIn("signature", repr(receipt).lower())

        self.assertEqual(len(client.status_calls), 1)
        self.assertEqual(len(client.redirect_calls), 1)
        self.assertEqual(len(session.calls), 1)
        _, kwargs = session.calls[0]
        self.assertIs(kwargs["stream"], True)
        self.assertIs(kwargs["allow_redirects"], False)
        self.assertEqual(kwargs["headers"], {})
        self.assertEqual(kwargs["cookies"], {})
        self.assertIsNone(kwargs["auth"])
        self.assertEqual(response.requested_chunk_sizes, [17])
        self.assertTrue(response.closed)

    def test_receipt_is_frozen(self) -> None:
        payload = make_zip()
        engine, _, _, _ = downloader(payload)
        with TemporaryDirectoryPath() as target:
            receipt = engine.download("fundamentals", "5", target)
            with self.assertRaises(FrozenInstanceError):
                receipt.sha256 = "changed"  # type: ignore[misc]

    def test_redirect_object_contract_is_supported(self) -> None:
        payload = make_zip()
        redirect = SimpleNamespace(location=SIGNED_URL, status_code=302)
        client = FakeClient(len(payload), redirect=redirect)
        engine, _, _, _ = downloader(payload, client=client)
        with TemporaryDirectoryPath() as target:
            self.assertTrue(engine.download("fundamentals", "10", target).artifact_path)

    def test_status_data_envelope_is_supported(self) -> None:
        payload = make_zip()
        client = FakeClient(
            len(payload),
            status={
                "data": {
                    "table": "fundamentals",
                    "name": "fundamentals.csv.zip",
                    "size": len(payload),
                    "modified": "2026-08-31T00:00:00Z",
                }
            },
        )
        engine, _, _, _ = downloader(payload, client=client)
        with TemporaryDirectoryPath() as target:
            self.assertEqual(
                engine.download("fundamentals", "full", target).received_bytes,
                len(payload),
            )

    def test_signed_object_request_rejects_credentialed_session(self) -> None:
        payload = make_zip()
        response = FakeResponse(payload)
        session = FakeSession(response, headers={"X-API-Key": "do-not-forward"})
        engine, _, _, _ = downloader(payload, session=session)
        with TemporaryDirectoryPath() as target:
            with self.assertRaisesRegex(BulkDownloadError, "contains credentials"):
                engine.download("fundamentals", "full", target)
            self.assertEqual(session.calls, [])
            self.assertEqual(list(target.iterdir()), [])

    def test_second_hop_rejects_all_inherited_credential_channels(self) -> None:
        payload = make_zip()

        def with_value(name: str, value: object) -> FakeSession:
            session = FakeSession(FakeResponse(payload))
            setattr(session, name, value)
            return session

        cases = (
            with_value("headers", {"Cookie": "sid=secret"}),
            with_value("cookies", {"sid": "secret"}),
            with_value("auth", ("user", "secret")),
            with_value("cert", "/secret/client.pem"),
            with_value("trust_env", True),
            with_value("params", {"api_key": "secret"}),
            with_value("params", {"harmless": "still-mutates-signed-url"}),
            with_value("hooks", {"response": [lambda response: response]}),
            with_value("proxies", {"https": "https://user:secret@proxy.invalid"}),
            with_value("proxies", {"https": "https://proxy.invalid?token=secret"}),
        )
        for session in cases:
            with self.subTest(channel=vars(session)):
                client = FakeClient(len(payload))
                engine = BulkDownloader(client, download_session=session)
                with TemporaryDirectoryPath() as target:
                    with self.assertRaises(BulkDownloadError):
                        engine.download("fundamentals", "full", target)
                    self.assertEqual(session.calls, [])
                    self.assertEqual(client.status_calls, [])
                    self.assertEqual(client.redirect_calls, [])
                    self.assertFalse(any(target.iterdir()))

    def test_default_second_hop_uses_one_time_environment_free_session(self) -> None:
        payload = make_zip()
        session = FakeSession(FakeResponse(payload))
        session.trust_env = True
        with patch("sharadar_pipeline.bulk.requests.Session", return_value=session):
            engine = BulkDownloader(FakeClient(len(payload)))
            with TemporaryDirectoryPath() as target:
                receipt = engine.download("fundamentals", "full", target)
                self.assertTrue(receipt.artifact_path.is_file())

        self.assertIs(session.trust_env, False)
        self.assertTrue(session.closed)
        self.assertEqual(len(session.calls), 1)

    def test_authenticated_client_session_cannot_be_reused_for_second_hop(self) -> None:
        payload = make_zip()
        response = FakeResponse(payload)
        session = FakeSession(response)
        client = FakeClient(len(payload))
        client._session = session  # type: ignore[attr-defined]

        with self.assertRaisesRegex(ValueError, "session separate"):
            BulkDownloader(client, download_session=session)

    def test_redirect_validation_fails_closed_without_leaking_url(self) -> None:
        payload = make_zip()
        unsafe_urls = (
            "http://objects.example.invalid/file.zip?secret=one",
            "https://user:pass@objects.example.invalid/file.zip?secret=two",
            "https://objects.example.invalid/file.zip#secret-three",
            "https:///missing-host.zip?secret=four",
        )
        for url in unsafe_urls:
            with self.subTest(url=url):
                client = FakeClient(len(payload), redirect=url)
                engine, _, session, _ = downloader(payload, client=client)
                with TemporaryDirectoryPath() as target:
                    with self.assertRaises(UnsafeBulkRedirect) as caught:
                        engine.download("fundamentals", "full", target)
                    self.assertNotIn("secret", str(caught.exception))
                    self.assertEqual(session.calls, [])

    def test_second_redirect_is_not_followed(self) -> None:
        payload = make_zip()
        response = FakeResponse(
            payload, status_code=307, headers={"Location": "https://elsewhere.invalid"}
        )
        engine, _, session, _ = downloader(payload, response=response)
        with TemporaryDirectoryPath() as target, self.assertRaises(UnsafeBulkRedirect):
            engine.download("fundamentals", "full", target)
        self.assertIs(session.calls[0][1]["allow_redirects"], False)

    def test_transport_and_stream_exceptions_are_sanitized(self) -> None:
        payload = make_zip()
        client = FakeClient(len(payload))
        cases = (
            FailingSession(),
            FakeSession(
                FakeResponse(
                    payload,
                    stream_error=RuntimeError(
                        f"stream broke while reading {SIGNED_URL}"
                    ),
                )
            ),
        )
        for session in cases:
            with self.subTest(session=type(session).__name__):
                engine = BulkDownloader(client, download_session=session)
                with TemporaryDirectoryPath() as target:
                    with self.assertRaises(BulkDownloadError) as caught:
                        engine.download("fundamentals", "full", target)
                    self.assertNotIn("signature", str(caught.exception).lower())
                    self.assertFalse(any(target.iterdir()))

    def test_status_and_content_length_must_agree(self) -> None:
        payload = make_zip()
        response = FakeResponse(
            payload, headers={"Content-Length": str(len(payload) + 1)}
        )
        engine, _, _, _ = downloader(payload, response=response)
        with TemporaryDirectoryPath() as target:
            with self.assertRaisesRegex(BulkIntegrityError, "disagree"):
                engine.download("fundamentals", "full", target)
            self.assertEqual(list(target.iterdir()), [])

    def test_actual_bytes_must_match_content_length(self) -> None:
        payload = make_zip()
        response = FakeResponse(
            payload, headers={"Content-Length": str(len(payload) + 1)}
        )
        status = {
            "table": "fundamentals",
            "name": "fundamentals.csv.zip",
            "size": len(payload) + 1,
        }
        engine, _, _, _ = downloader(
            payload, response=response, client=FakeClient(len(payload), status=status)
        )
        with TemporaryDirectoryPath() as target:
            with self.assertRaisesRegex(BulkIntegrityError, "received bytes"):
                engine.download("fundamentals", "full", target)
            self.assertFalse(any(target.iterdir()))

    def test_malformed_content_length_and_encoded_payload_are_rejected(self) -> None:
        payload = make_zip()
        cases = (
            ({"Content-Length": "12x"}, "Content-Length is invalid"),
            (
                {"Content-Length": str(len(payload)), "Content-Encoding": "gzip"},
                "content encoding",
            ),
        )
        for headers, message in cases:
            with self.subTest(headers=headers):
                response = FakeResponse(payload, headers=headers)
                engine, _, _, _ = downloader(payload, response=response)
                with (
                    TemporaryDirectoryPath() as target,
                    self.assertRaisesRegex(BulkIntegrityError, message),
                ):
                    engine.download("fundamentals", "full", target)

    def test_vendor_metadata_cannot_pollute_receipt_or_manifest(self) -> None:
        payload = make_zip()
        vendor_marker = "vendor-controlled-marker"
        status = {
            "table": "fundamentals",
            "name": f"../{vendor_marker}.zip",
            "modified": f"2099-99-99T99:99:99Z-{vendor_marker}",
            "size": len(payload),
        }
        engine, _, _, _ = downloader(
            payload, client=FakeClient(len(payload), status=status)
        )
        with TemporaryDirectoryPath() as target:
            receipt = engine.download("fundamentals", "full", target)
            serialized = json.dumps(receipt.manifest())
            self.assertEqual(receipt.source_name, "fundamentals.full.csv.zip")
            self.assertNotIn(vendor_marker, serialized)
            self.assertNotIn("modified", serialized)

    def test_response_filename_cannot_escape_destination(self) -> None:
        payload = make_zip()
        response = FakeResponse(
            payload,
            headers={
                "Content-Length": str(len(payload)),
                "Content-Disposition": 'attachment; filename="../../stolen.zip"',
            },
        )
        engine, _, _, _ = downloader(payload, response=response)
        with TemporaryDirectoryPath() as target:
            with self.assertRaisesRegex(BulkIntegrityError, "filename"):
                engine.download("fundamentals", "full", target)
            self.assertFalse(any(target.iterdir()))

    def test_non_zip_crc_failure_and_member_traversal_cleanup_partials(self) -> None:
        valid = make_zip(rows=b"ticker,date\nAAPL,2026-08-31\n")
        corrupted = bytearray(valid)
        data_at = valid.index(b"AAPL")
        corrupted[data_at] = ord("X")
        cases = (
            b"not a zip",
            bytes(corrupted),
            make_zip(member="../fundamentals.csv"),
            make_zip(member="data.txt"),
        )
        for payload in cases:
            with self.subTest(payload_length=len(payload)):
                engine, _, _, response = downloader(payload)
                with TemporaryDirectoryPath() as target:
                    with self.assertRaises(BulkIntegrityError):
                        engine.download("fundamentals", "full", target)
                    self.assertFalse(any(target.iterdir()))
                self.assertTrue(response.closed)

    def test_zip_requires_one_root_csv_bound_to_requested_table(self) -> None:
        valid_rows = b"ticker,date\nAAPL,2026-08-31\n"
        cases = (
            make_zip_entries(
                (
                    ("fundamentals.csv", valid_rows),
                    ("extra.csv", valid_rows),
                )
            ),
            make_zip(member="nested/fundamentals.csv"),
            make_zip(member="stocks.csv"),
            make_zip_entries(
                (
                    ("fundamentals.csv", valid_rows),
                    ("FUNDAMENTALS.CSV", valid_rows),
                )
            ),
        )
        for payload in cases:
            with self.subTest(payload_length=len(payload)):
                engine = BulkDownloader(
                    FakeClient(len(payload)),
                    download_session=FakeSession(FakeResponse(payload)),
                    max_zip_members=2,
                )
                with TemporaryDirectoryPath() as target:
                    with self.assertRaises(BulkIntegrityError):
                        engine.download("fundamentals", "full", target)
                    self.assertFalse(any(target.iterdir()))

    def test_code_owned_legacy_member_name_remains_table_bound(self) -> None:
        payload = make_zip(member="SHARADAR_SF1.csv")
        engine, _, _, _ = downloader(payload)
        with TemporaryDirectoryPath() as target:
            receipt = engine.download("fundamentals", "full", target)
            self.assertEqual(receipt.csv_members, ("SHARADAR_SF1.csv",))

    def test_expected_schema_policy_matches_exact_header_before_promotion(self) -> None:
        payload = make_zip()
        good = BulkDownloader(
            FakeClient(len(payload)),
            download_session=FakeSession(FakeResponse(payload)),
            expected_headers={"SF1": ("ticker", "date", "value")},
        )
        bad = BulkDownloader(
            FakeClient(len(payload)),
            download_session=FakeSession(FakeResponse(payload)),
            expected_headers={"fundamentals": ("ticker", "date", "revenue")},
        )
        with TemporaryDirectoryPath() as good_target:
            self.assertTrue(
                good.download("fundamentals", "full", good_target).artifact_path
            )
        with TemporaryDirectoryPath() as bad_target:
            with self.assertRaisesRegex(BulkIntegrityError, "schema policy"):
                bad.download("fundamentals", "full", bad_target)
            self.assertFalse(any(bad_target.iterdir()))

    def test_zip_resource_caps_reject_declared_size_and_ratio_before_promotion(
        self,
    ) -> None:
        normal = make_zip()
        compressed = make_zip(
            rows=b"ticker,date\n" + (b"AAPL,2026-08-31\n" * 20_000),
            compression=zipfile.ZIP_DEFLATED,
        )
        cases = (
            (
                normal,
                {"max_uncompressed_bytes": 8},
                "uncompressed-byte limit",
            ),
            (
                compressed,
                {"max_compression_ratio": 2.0},
                "compression-ratio limit",
            ),
        )
        for payload, limits, message in cases:
            with self.subTest(message=message):
                engine = BulkDownloader(
                    FakeClient(len(payload)),
                    download_session=FakeSession(FakeResponse(payload)),
                    **limits,
                )
                with TemporaryDirectoryPath() as target:
                    with self.assertRaisesRegex(BulkIntegrityError, message):
                        engine.download("fundamentals", "full", target)
                    self.assertFalse(any(target.iterdir()))

    def test_atomic_artifact_and_manifest_renames_fsync_directory(self) -> None:
        payload = make_zip()
        engine, _, _, _ = downloader(payload)
        with (
            patch("sharadar_pipeline.bulk._fsync_directory") as fsync_directory,
            TemporaryDirectoryPath() as target,
        ):
            engine.download("fundamentals", "full", target)
            self.assertEqual(fsync_directory.call_count, 2)
            self.assertEqual(
                [call.args[0] for call in fsync_directory.call_args_list],
                [target, target],
            )

    def test_csv_header_validation(self) -> None:
        cases = (
            b"only_one\nvalue\n",
            b"ticker,ticker\nAAPL,AAPL\n",
            b"ticker,\nAAPL,1\n",
            b"\xff,other\nAAPL,1\n",
        )
        for rows in cases:
            payload = make_zip(rows=rows)
            engine, _, _, _ = downloader(payload)
            with (
                self.subTest(rows=rows),
                TemporaryDirectoryPath() as target,
                self.assertRaises(BulkIntegrityError),
            ):
                engine.download("fundamentals", "full", target)

    def test_same_content_replay_is_idempotent(self) -> None:
        payload = make_zip()
        first = FakeResponse(payload)
        second = FakeResponse(payload)
        session = FakeSession(first, second)
        client = FakeClient(len(payload))
        engine = BulkDownloader(client, download_session=session, chunk_size=16)
        with TemporaryDirectoryPath() as target:
            receipt_one = engine.download("fundamentals", "full", target)
            receipt_two = engine.download("fundamentals", "full", target)

            self.assertFalse(receipt_one.replayed)
            self.assertTrue(receipt_two.replayed)
            self.assertEqual(receipt_one.artifact_path, receipt_two.artifact_path)
            self.assertEqual(receipt_one.manifest(), receipt_two.manifest())
            self.assertEqual(len(list(target.glob("*.zip"))), 1)
            self.assertEqual(len(list(target.glob("*.manifest.json"))), 1)

    def test_changed_content_preserves_both_immutable_captures(self) -> None:
        first_payload = make_zip(rows=b"ticker,date\nAAPL,2026-08-30\n")
        second_payload = make_zip(rows=b"ticker,date\nAAPL,2026-08-31\n")
        session = FakeSession(FakeResponse(first_payload), FakeResponse(second_payload))
        client = FakeClient(len(first_payload))
        engine = BulkDownloader(client, download_session=session)
        with TemporaryDirectoryPath() as target:
            first = engine.download("fundamentals", "full", target)
            client.status["size"] = len(second_payload)
            second = engine.download("fundamentals", "full", target)

            self.assertNotEqual(first.sha256, second.sha256)
            self.assertNotEqual(first.artifact_path, second.artifact_path)
            self.assertEqual(len(list(target.glob("*.zip"))), 2)

    def test_custom_transport_cannot_break_bounded_chunk_contract(self) -> None:
        payload = make_zip()
        response = FakeResponse(payload, oversized_chunk=True)
        engine, _, _, _ = downloader(payload, response=response, chunk_size=8)
        with TemporaryDirectoryPath() as target:
            with self.assertRaisesRegex(BulkDownloadError, "chunk size"):
                engine.download("fundamentals", "full", target)
            self.assertFalse(any(target.iterdir()))

    @pytest.mark.performance
    def test_stream_operation_count_scales_linearly_and_reads_are_bounded(self) -> None:
        # Stored data keeps the ZIP large enough to exercise streaming without a
        # timing assertion, which would be flaky on shared CI hosts.
        rows = b"ticker,date,value\n" + (b"AAPL,2026-08-31,123456789\n" * 80_000)
        payload = make_zip(rows=rows)
        chunk_size = 64 * 1024
        response = FakeResponse(payload)
        engine, _, _, _ = downloader(payload, response=response, chunk_size=chunk_size)
        with TemporaryDirectoryPath() as target:
            receipt = engine.download("fundamentals", "full", target)

        self.assertEqual(response.requested_chunk_sizes, [chunk_size])
        self.assertEqual(response.yield_count, math.ceil(len(payload) / chunk_size))
        self.assertEqual(receipt.received_bytes, len(payload))

    def test_constructor_and_input_validation(self) -> None:
        payload = make_zip()
        client = FakeClient(len(payload))
        session = FakeSession(FakeResponse(payload))
        for invalid in (0, -1, True, 1.5):
            with self.subTest(chunk_size=invalid), self.assertRaises(ValueError):
                BulkDownloader(client, download_session=session, chunk_size=invalid)  # type: ignore[arg-type]
        for parameter in (
            {"max_zip_members": 0},
            {"max_uncompressed_bytes": 0},
            {"max_compression_ratio": 0.5},
            {"max_compression_ratio": math.nan},
            {"max_compression_ratio": math.inf},
        ):
            with self.subTest(parameter=parameter), self.assertRaises(ValueError):
                BulkDownloader(client, download_session=session, **parameter)
        for timeout in (
            (True, 1.0),
            (math.nan, 1.0),
            (1.0, math.inf),
        ):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                BulkDownloader(client, download_session=session, timeout=timeout)
        engine = BulkDownloader(client, download_session=session)
        with TemporaryDirectoryPath() as target, self.assertRaises(ValueError):
            engine.download("fundamentals", "all", target)


class TemporaryDirectoryPath:
    """Tiny pathlib wrapper that keeps test setup dependency-free."""

    def __init__(self) -> None:
        import tempfile

        self._temporary = tempfile.TemporaryDirectory()

    def __enter__(self) -> Path:
        return Path(self._temporary.__enter__())

    def __exit__(self, *args: object) -> object:
        return self._temporary.__exit__(*args)
