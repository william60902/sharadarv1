"""Secure, streaming Sharadar bulk-download primitives.

The authenticated API request and the signed-object download are deliberately
separate.  ``SharadarClient.bulk_redirect`` owns the authenticated first hop;
this module validates the returned location and performs the second hop with a
credential-free session.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import stat
import tempfile
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Final, Protocol
from urllib.parse import unquote, urlsplit

import requests

from .catalog import HistoryWindow, SharadarTable, normalize_table, table_spec

DEFAULT_CHUNK_SIZE: Final = 1024 * 1024
MAX_CSV_HEADER_BYTES: Final = 1024 * 1024
DEFAULT_MAX_ZIP_MEMBERS: Final = 1
DEFAULT_MAX_UNCOMPRESSED_BYTES: Final = 64 * 1024 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO: Final = 200.0
_ZIP_MAGICS: Final = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
_SENSITIVE_HEADERS: Final = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-api-key",
        "api-key",
        "apikey",
    }
)
_MINIMUM_HEADER_FIELDS: Final = {
    table: frozenset({"table"})
    if table is SharadarTable.DESCRIPTIONS
    else frozenset({"ticker", "date"})
    if table
    in {
        SharadarTable.FUNDAMENTALS,
        SharadarTable.DAILY,
        SharadarTable.ACTIONS,
        SharadarTable.EVENTS,
        SharadarTable.SP500,
        SharadarTable.STOCKS,
        SharadarTable.FUNDS,
        SharadarTable.METRICS,
    }
    else frozenset({"ticker"})
    for table in SharadarTable
}


class BulkDownloadError(RuntimeError):
    """Base error whose messages intentionally exclude signed URLs."""


class UnsafeBulkRedirect(BulkDownloadError):
    """The first-hop redirect is unsafe for a credential-like signed URL."""


class BulkIntegrityError(BulkDownloadError):
    """The downloaded object failed byte, ZIP, member, or CSV validation."""


class BulkClient(Protocol):
    def bulk_status(self, table: SharadarTable | str) -> object: ...

    def bulk_redirect(
        self, table: SharadarTable | str, years: HistoryWindow | str
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class BulkDownloadReceipt:
    """Immutable result and URL-free manifest data for one bulk object."""

    table: str
    history: str
    source_name: str
    artifact_path: Path
    manifest_path: Path
    status_reported_bytes: int | None
    received_bytes: int
    sha256: str
    zip_members: tuple[str, ...]
    csv_members: tuple[str, ...]
    csv_headers: tuple[tuple[str, ...], ...]
    replayed: bool

    def manifest(self) -> dict[str, object]:
        """Return the stable, serializable manifest (never a signed URL)."""

        return {
            "manifest_version": 1,
            "source": "sharadar",
            "table": self.table,
            "history": self.history,
            "source_name": self.source_name,
            "artifact_file": self.artifact_path.name,
            "status_reported_bytes": self.status_reported_bytes,
            "received_bytes": self.received_bytes,
            "sha256": self.sha256,
            "zip_crc_ok": True,
            "zip_members": list(self.zip_members),
            "csv_members": list(self.csv_members),
            "csv_headers": [list(header) for header in self.csv_headers],
        }


@dataclass(frozen=True, slots=True)
class _StatusMetadata:
    table: str | None
    size: int | None


class BulkDownloader:
    """Download a Sharadar bulk ZIP with bounded-memory, linear passes."""

    def __init__(
        self,
        client: BulkClient,
        *,
        download_session: Any | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: tuple[float, float] = (10.0, 300.0),
        expected_headers: Mapping[SharadarTable | str, Sequence[str]] | None = None,
        max_zip_members: int = DEFAULT_MAX_ZIP_MEMBERS,
        max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
    ) -> None:
        if (
            not isinstance(chunk_size, int)
            or isinstance(chunk_size, bool)
            or chunk_size <= 0
        ):
            raise ValueError("chunk_size must be a positive integer")
        if (
            not isinstance(timeout, tuple)
            or len(timeout) != 2
            or any(
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
                for value in timeout
            )
        ):
            raise ValueError("timeout must be a positive (connect, read) pair")
        for name, value in (
            ("max_zip_members", max_zip_members),
            ("max_uncompressed_bytes", max_uncompressed_bytes),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if (
            not isinstance(max_compression_ratio, (int, float))
            or isinstance(max_compression_ratio, bool)
            or not math.isfinite(max_compression_ratio)
            or max_compression_ratio < 1.0
        ):
            raise ValueError("max_compression_ratio must be at least 1.0")
        authenticated_session = getattr(client, "_session", None)
        if (
            download_session is not None
            and authenticated_session is not None
            and download_session is authenticated_session
        ):
            raise ValueError(
                "bulk objects require a session separate from the authenticated client"
            )

        self._client = client
        # A non-injected second hop gets a new credential-free session for each
        # object.  It is closed after that object and never shares client state.
        self._download_session = download_session
        self._chunk_size = chunk_size
        self._timeout = timeout
        self._expected_headers = _normalize_expected_headers(expected_headers)
        self._max_zip_members = max_zip_members
        self._max_uncompressed_bytes = max_uncompressed_bytes
        self._max_compression_ratio = float(max_compression_ratio)

    def download(
        self,
        table: SharadarTable | str,
        years: HistoryWindow | str,
        destination_dir: str | os.PathLike[str],
    ) -> BulkDownloadReceipt:
        """Stream, verify, and atomically promote one immutable bulk ZIP."""

        exact_table = normalize_table(table)
        try:
            exact_history = HistoryWindow(years)
        except (TypeError, ValueError):
            raise ValueError("years must be one of: 5, 10, full") from None

        # Reject unsafe injected state before either the authenticated status
        # request or the signed-object request can touch the network.
        if self._download_session is not None:
            _ensure_session_has_no_credentials(self._download_session)

        status = _parse_status(self._client.bulk_status(exact_table))
        if status.table is not None:
            try:
                reported_table = normalize_table(status.table)
            except ValueError:
                raise BulkIntegrityError(
                    "bulk status reported an invalid table"
                ) from None
            if reported_table is not exact_table:
                raise BulkIntegrityError("bulk status table does not match the request")

        redirect_url = _extract_redirect_url(
            self._client.bulk_redirect(exact_table, exact_history)
        )
        _validate_https_url(redirect_url)

        destination = Path(destination_dir)
        destination.mkdir(parents=True, exist_ok=True)
        if not destination.is_dir():
            raise BulkDownloadError("bulk destination is not a directory")

        partial_path: Path | None = None
        response: Any | None = None
        promoted = False
        replayed = False
        artifact_path: Path | None = None
        manifest_path: Path | None = None
        download_session = self._download_session
        owns_download_session = download_session is None
        if download_session is None:
            download_session = requests.Session()
            # Disable environment netrc/proxy credential discovery. Explicit
            # non-credentialed proxies remain possible via injected sessions.
            download_session.trust_env = False
        try:
            _ensure_session_has_no_credentials(download_session)
            try:
                response = download_session.get(
                    redirect_url,
                    stream=True,
                    allow_redirects=False,
                    timeout=self._timeout,
                    headers={},
                    cookies={},
                    auth=None,
                )
            except Exception:  # noqa: BLE001 - sanitize transport URL-bearing errors
                # Transport exceptions often embed request URLs.  The signed
                # location is credential-like, so suppress that exception text.
                raise BulkDownloadError(
                    "bulk object download transport failed"
                ) from None
            status_code = getattr(response, "status_code", None)
            if status_code != 200:
                if isinstance(status_code, int) and 300 <= status_code < 400:
                    raise UnsafeBulkRedirect(
                        "bulk object host returned another redirect"
                    )
                raise BulkDownloadError(
                    "bulk object download returned a non-200 response"
                )

            headers = getattr(response, "headers", {}) or {}
            encoding = _header(headers, "Content-Encoding")
            if encoding and encoding.strip().lower() != "identity":
                raise BulkIntegrityError(
                    "bulk object used an unsupported content encoding"
                )

            response_name = _content_disposition_filename(
                _header(headers, "Content-Disposition")
            )
            if response_name is not None:
                _safe_filename(response_name)
            # Vendor-controlled names and timestamps are intentionally not
            # persisted. The canonical name binds the object to code-owned
            # table/history identities before promotion.
            source_name = _source_name(exact_table, exact_history)

            response_size = _content_length(_header(headers, "Content-Length"))
            expected_size = status.size
            if (
                expected_size is not None
                and response_size is not None
                and expected_size != response_size
            ):
                raise BulkIntegrityError(
                    "bulk status size and response Content-Length disagree"
                )

            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=".sharadar-bulk-",
                suffix=".partial",
                dir=destination,
                delete=False,
            ) as temporary:
                partial_path = Path(temporary.name)
                received_bytes, digest = _stream_and_hash(
                    response, temporary, chunk_size=self._chunk_size
                )
                temporary.flush()
                os.fsync(temporary.fileno())

            for label, size in (
                ("bulk status size", expected_size),
                ("response Content-Length", response_size),
            ):
                if size is not None and received_bytes != size:
                    raise BulkIntegrityError(f"received bytes do not match {label}")

            zip_members, csv_members, csv_headers = _validate_zip(
                partial_path,
                expected_table=exact_table,
                expected_header=self._expected_headers.get(exact_table),
                max_members=self._max_zip_members,
                max_uncompressed_bytes=self._max_uncompressed_bytes,
                max_compression_ratio=self._max_compression_ratio,
            )
            artifact_name = _content_addressed_name(source_name, digest)
            artifact_path = destination / artifact_name
            manifest_path = destination / f"{artifact_name}.manifest.json"

            if artifact_path.exists() or artifact_path.is_symlink():
                if not artifact_path.is_file() or artifact_path.is_symlink():
                    raise BulkIntegrityError(
                        "immutable artifact target is not a regular file"
                    )
                if (
                    artifact_path.stat().st_size != received_bytes
                    or _sha256_file(artifact_path) != digest
                ):
                    raise BulkIntegrityError(
                        "immutable artifact target has unexpected content"
                    )
                replayed = True
                partial_path.unlink()
                partial_path = None
            else:
                os.replace(partial_path, artifact_path)
                partial_path = None
                promoted = True
                _fsync_directory(destination)

            receipt = BulkDownloadReceipt(
                table=exact_table.value,
                history=exact_history.value,
                source_name=source_name,
                artifact_path=artifact_path,
                manifest_path=manifest_path,
                status_reported_bytes=expected_size,
                received_bytes=received_bytes,
                sha256=digest,
                zip_members=zip_members,
                csv_members=csv_members,
                csv_headers=csv_headers,
                replayed=replayed,
            )
            _write_manifest_once(manifest_path, receipt.manifest())
            return receipt
        except Exception:
            if partial_path is not None:
                partial_path.unlink(missing_ok=True)
            if promoted and artifact_path is not None:
                artifact_path.unlink(missing_ok=True)
            if promoted and manifest_path is not None:
                manifest_path.unlink(missing_ok=True)
            raise
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()
            if owns_download_session:
                close_session = getattr(download_session, "close", None)
                if callable(close_session):
                    close_session()


def _read_field(value: object, *names: str) -> object | None:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        data = value.get("data")
        if isinstance(data, Mapping):
            return _read_field(data, *names)
        return None
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _parse_status(value: object) -> _StatusMetadata:
    table = _read_field(value, "table")
    raw_size = _read_field(value, "size", "bytes", "file_size", "filesize")

    if table is not None and not isinstance(table, (str, SharadarTable)):
        raise BulkIntegrityError("bulk status table has an invalid type")
    size: int | None = None
    if raw_size is not None:
        if not isinstance(raw_size, int) or isinstance(raw_size, bool) or raw_size < 0:
            raise BulkIntegrityError("bulk status size must be a non-negative integer")
        size = raw_size
    return _StatusMetadata(
        table=str(table) if table is not None else None,
        size=size,
    )


def _extract_redirect_url(value: object) -> str:
    if isinstance(value, str):
        return value

    status_code = getattr(value, "status_code", None)
    if status_code is not None and status_code != 302:
        raise BulkDownloadError("bulk API did not return the required 302 redirect")
    location = _read_field(value, "location", "redirect_url")
    if location is None:
        headers = getattr(value, "headers", None)
        if isinstance(headers, Mapping):
            location = _header(headers, "Location")
    if not isinstance(location, str) or not location:
        raise BulkDownloadError("bulk API response did not contain a redirect location")
    return location


def _validate_https_url(url: str) -> None:
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as exc:
        raise UnsafeBulkRedirect("bulk redirect URL is malformed") from exc
    if parts.scheme.lower() != "https" or not parts.hostname:
        raise UnsafeBulkRedirect("bulk redirect must use HTTPS with a valid host")
    if parts.username is not None or parts.password is not None:
        raise UnsafeBulkRedirect("bulk redirect must not contain user information")
    if port is not None and not 1 <= port <= 65535:
        raise UnsafeBulkRedirect("bulk redirect contains an invalid port")
    if parts.fragment:
        raise UnsafeBulkRedirect("bulk redirect must not contain a fragment")


def _ensure_session_has_no_credentials(session: object) -> None:
    headers = getattr(session, "headers", None)
    if isinstance(headers, Mapping):
        for name, value in headers.items():
            normalized = str(name).strip().lower()
            if value is not None and _credential_name(normalized):
                raise BulkDownloadError(
                    "download session contains credentials and cannot fetch signed objects"
                )

    cookies = getattr(session, "cookies", None)
    if cookies is not None:
        try:
            has_cookies = len(cookies) > 0
        except TypeError:
            has_cookies = bool(cookies)
        if has_cookies:
            raise BulkDownloadError("download session must not contain cookies")

    if getattr(session, "auth", None) is not None:
        raise BulkDownloadError("download session must not contain authentication")
    if getattr(session, "cert", None) not in (None, "", ()):
        raise BulkDownloadError(
            "download session must not contain a client certificate"
        )
    if bool(getattr(session, "trust_env", False)):
        raise BulkDownloadError(
            "download session must disable environment credential discovery"
        )

    params = getattr(session, "params", None)
    if isinstance(params, Mapping) and params:
        # Any inherited parameter mutates the signed URL. Credential-named
        # values are especially dangerous, but even benign ones invalidate the
        # signature and therefore fail closed.
        raise BulkDownloadError("download session must not inherit query parameters")

    hooks = getattr(session, "hooks", None)
    if hooks and (
        not isinstance(hooks, Mapping)
        or any(bool(callbacks) for callbacks in hooks.values())
    ):
        raise BulkDownloadError("download session must not contain active hooks")

    proxies = getattr(session, "proxies", None)
    if proxies:
        # Proxy configuration can inject Proxy-Authorization outside the visible
        # request headers. A signed object may use a purpose-built transport,
        # but this strict downloader only accepts a direct second hop.
        raise BulkDownloadError("download session must not contain proxies")


def _credential_name(value: str) -> bool:
    return value in _SENSITIVE_HEADERS or ("api" in value and "key" in value)


def _header(headers: Mapping[object, object], name: str) -> str | None:
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            return str(value)
    return None


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    if not re.fullmatch(r"[0-9]+", value.strip()):
        raise BulkIntegrityError("response Content-Length is invalid")
    return int(value)


def _content_disposition_filename(value: str | None) -> str | None:
    if not value:
        return None
    extended = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", value, re.IGNORECASE)
    if extended:
        return unquote(extended.group(1).strip())
    quoted = re.search(r'filename\s*=\s*"([^"]*)"', value, re.IGNORECASE)
    if quoted:
        return quoted.group(1)
    plain = re.search(r"filename\s*=\s*([^;\s]+)", value, re.IGNORECASE)
    return plain.group(1) if plain else None


def _safe_filename(value: str) -> str:
    if not value or value in {".", ".."} or "\x00" in value:
        raise BulkIntegrityError("bulk filename is unsafe")
    if (
        value != Path(value).name
        or value != PureWindowsPath(value).name
        or "/" in value
        or "\\" in value
    ):
        raise BulkIntegrityError("bulk filename contains a path")
    if any(ord(character) < 32 for character in value):
        raise BulkIntegrityError("bulk filename contains control characters")
    if not value.lower().endswith(".zip"):
        raise BulkIntegrityError("bulk filename must end in .zip")
    return value


def _source_name(table: SharadarTable, history: HistoryWindow) -> str:
    """Return a code-owned artifact name with no vendor-controlled text."""

    return f"{table.value}.{history.value}.csv.zip"


def _normalize_expected_headers(
    value: Mapping[SharadarTable | str, Sequence[str]] | None,
) -> dict[SharadarTable, tuple[str, ...]]:
    normalized: dict[SharadarTable, tuple[str, ...]] = {}
    for raw_table, raw_header in (value or {}).items():
        exact_table = normalize_table(raw_table)
        if exact_table in normalized:
            raise ValueError("expected_headers contains duplicate table aliases")
        if isinstance(raw_header, (str, bytes)):
            raise TypeError("an expected CSV header must be a sequence of fields")
        header = tuple(raw_header)
        if any(not isinstance(field, str) for field in header):
            raise ValueError("expected CSV header fields must be strings")
        try:
            normalized[exact_table] = _validate_header_fields(header)
        except BulkIntegrityError as exc:
            raise ValueError("expected CSV header policy is invalid") from exc
    return normalized


def _stream_and_hash(
    response: object, output: Any, *, chunk_size: int
) -> tuple[int, str]:
    iterator = getattr(response, "iter_content", None)
    if not callable(iterator):
        raise BulkDownloadError("bulk response does not support streaming")
    digest = hashlib.sha256()
    received = 0
    try:
        for chunk in iterator(chunk_size=chunk_size):
            if not chunk:
                continue
            if not isinstance(chunk, bytes):
                raise BulkDownloadError("bulk response yielded a non-bytes chunk")
            # requests honours chunk_size.  Enforce it as a hard invariant for
            # custom transports too so one bad adapter cannot defeat bounded RAM.
            if len(chunk) > chunk_size:
                raise BulkDownloadError(
                    "bulk response exceeded the configured chunk size"
                )
            output.write(chunk)
            digest.update(chunk)
            received += len(chunk)
    except BulkDownloadError:
        raise
    except Exception:  # noqa: BLE001 - sanitize transport URL-bearing errors
        raise BulkDownloadError("bulk object stream failed") from None
    return received, digest.hexdigest()


def _validate_member_name(name: str) -> None:
    if not name or "\x00" in name:
        raise BulkIntegrityError("ZIP contains an unsafe member name")
    posix = PurePosixPath(name)
    windows = PureWindowsPath(name)
    if (
        posix.is_absolute()
        or windows.is_absolute()
        or windows.drive
        or ".." in posix.parts
        or ".." in windows.parts
    ):
        raise BulkIntegrityError("ZIP member attempts path traversal")


def _validate_zip(
    path: Path,
    *,
    expected_table: SharadarTable,
    expected_header: tuple[str, ...] | None,
    max_members: int,
    max_uncompressed_bytes: int,
    max_compression_ratio: float,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[tuple[str, ...], ...]]:
    with path.open("rb") as stream:
        if stream.read(4) not in _ZIP_MAGICS:
            raise BulkIntegrityError("bulk object does not have a ZIP signature")

    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            if not infos:
                raise BulkIntegrityError("bulk ZIP contains no members")
            if len(infos) > max_members:
                raise BulkIntegrityError("bulk ZIP exceeds the member-count limit")
            names = [info.filename for info in infos]
            casefold_names = [name.casefold() for name in names]
            if len(casefold_names) != len(set(casefold_names)):
                raise BulkIntegrityError("bulk ZIP contains colliding member names")
            if len(infos) != 1:
                raise BulkIntegrityError(
                    "bulk ZIP must contain exactly one root-level CSV"
                )

            total_uncompressed = 0
            for info in infos:
                _validate_member_name(info.filename)
                mode = info.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise BulkIntegrityError("ZIP contains a symbolic-link member")
                if info.flag_bits & 0x1:
                    raise BulkIntegrityError("ZIP contains an encrypted member")
                if info.is_dir() or "/" in info.filename or "\\" in info.filename:
                    raise BulkIntegrityError("bulk ZIP must contain one root-level CSV")
                if not info.filename.lower().endswith(".csv"):
                    raise BulkIntegrityError("bulk ZIP member must be a CSV")
                if info.filename.casefold() not in _expected_member_names(
                    expected_table
                ):
                    raise BulkIntegrityError(
                        "bulk ZIP member does not match the requested table"
                    )
                total_uncompressed += info.file_size
                if total_uncompressed > max_uncompressed_bytes:
                    raise BulkIntegrityError(
                        "bulk ZIP exceeds the uncompressed-byte limit"
                    )
                if info.file_size:
                    ratio = info.file_size / max(1, info.compress_size)
                    if ratio > max_compression_ratio:
                        raise BulkIntegrityError(
                            "bulk ZIP exceeds the compression-ratio limit"
                        )

            bad_member = archive.testzip()
            if bad_member is not None:
                raise BulkIntegrityError("bulk ZIP failed CRC validation")

            header = _read_csv_header(archive, infos[0])
            if expected_header is not None and header != expected_header:
                raise BulkIntegrityError(
                    "CSV header does not match the expected schema policy"
                )
            minimum_fields = _MINIMUM_HEADER_FIELDS.get(expected_table, frozenset())
            if not minimum_fields.issubset(field.casefold() for field in header):
                raise BulkIntegrityError(
                    "CSV header does not match the table identity policy"
                )
            return (
                tuple(info.filename for info in infos),
                (infos[0].filename,),
                (header,),
            )
    except BulkIntegrityError:
        raise
    except (OSError, RuntimeError, UnicodeError, zipfile.BadZipFile) as exc:
        raise BulkIntegrityError("bulk ZIP could not be validated") from exc


def _read_csv_header(
    archive: zipfile.ZipFile, info: zipfile.ZipInfo
) -> tuple[str, ...]:
    with archive.open(info, "r") as member:
        raw_header = member.readline(MAX_CSV_HEADER_BYTES + 1)
    if len(raw_header) > MAX_CSV_HEADER_BYTES:
        raise BulkIntegrityError("CSV header exceeds the safety limit")
    if not raw_header:
        raise BulkIntegrityError("CSV member has no header")
    try:
        text_header = raw_header.decode("utf-8-sig")
        fields = next(csv.reader([text_header]))
    except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
        raise BulkIntegrityError("CSV header is not valid UTF-8 CSV") from exc
    return _validate_header_fields(tuple(field.strip() for field in fields))


def _expected_member_names(table: SharadarTable) -> frozenset[str]:
    stems = {table.value, *table_spec(table).legacy_aliases}
    return frozenset(
        name.casefold()
        for stem in stems
        for name in (f"{stem}.csv", f"SHARADAR_{stem}.csv")
    )


def _validate_header_fields(fields: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(field.strip() for field in fields)
    if len(normalized) < 2 or any(not field for field in normalized):
        raise BulkIntegrityError("CSV header must contain at least two named columns")
    if len(set(normalized)) != len(normalized):
        raise BulkIntegrityError("CSV header contains duplicate columns")
    if any(any(ord(character) < 32 for character in field) for field in normalized):
        raise BulkIntegrityError("CSV header contains control characters")
    return normalized


def _content_addressed_name(source_name: str, digest: str) -> str:
    base = source_name[:-4]
    return f"{base}.{digest}.zip"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(DEFAULT_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _write_manifest_once(path: Path, manifest: Mapping[str, object]) -> None:
    encoded = (
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != encoded:
            raise BulkIntegrityError("immutable manifest target has unexpected content")
        return

    partial: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=".sharadar-manifest-",
            suffix=".partial",
            dir=path.parent,
            delete=False,
        ) as temporary:
            partial = Path(temporary.name)
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(partial, path)
        partial = None
        _fsync_directory(path.parent)
    finally:
        if partial is not None:
            partial.unlink(missing_ok=True)


def _fsync_directory(directory: Path) -> None:
    """Make the preceding same-directory atomic rename crash-durable."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(directory, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
