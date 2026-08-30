"""Safe, typed failures raised by the Sharadar HTTP client.

The exceptions in this module deliberately retain operational metadata only.  They
never retain a request, response, API key, response body, or signed bulk URL.
"""

from __future__ import annotations

import re
from typing import Final

_SENSITIVE_QUERY: Final[re.Pattern[str]] = re.compile(
    r"(?i)(api[_-]?key|apikey|x-api-key|token|signature|x-amz-[^=&\s]+)"
    r"(\s*[=:]\s*|=)[^&\s,;]+"
)
_AUTHORIZATION: Final[re.Pattern[str]] = re.compile(
    r"(?i)\b(authorization|proxy-authorization)\s*[:=]\s*"
    r"(?:bearer\s+)?[^\s,;]+"
)
_BEARER: Final[re.Pattern[str]] = re.compile(
    r"(?i)\bbearer\s+[^\s,;]+"
)
_COOKIE: Final[re.Pattern[str]] = re.compile(
    r"(?i)\b(set-cookie|cookie)\s*[:=]\s*[^\r\n]+"
)
_URL: Final[re.Pattern[str]] = re.compile(r"https?://[^\s]+", re.IGNORECASE)


def redact_sensitive(value: object) -> str:
    """Return a log-safe rendering without credentials or complete URLs."""

    text = str(value)
    text = _COOKIE.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    text = _AUTHORIZATION.sub(
        lambda match: f"{match.group(1)}=<redacted>", text
    )
    text = _BEARER.sub("Bearer <redacted>", text)
    text = _SENSITIVE_QUERY.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    # Bulk locations are pre-signed and credential-like.  Redact *all* complete
    # URLs so an upstream error cannot accidentally disclose one.
    return _URL.sub("<redacted-url>", text)


class SharadarError(RuntimeError):
    """Base class whose ``str`` and ``repr`` are safe to log."""

    default_message = "Sharadar request failed"

    def __init__(
        self,
        message: str | None = None,
        *,
        status_code: int | None = None,
        retryable: bool = False,
    ) -> None:
        self.safe_message = redact_sensitive(message or self.default_message)
        self.status_code = status_code
        self.retryable = retryable
        # RuntimeError stores only the already-redacted value in ``args``.
        super().__init__(self.safe_message)

    def __repr__(self) -> str:
        metadata = []
        if self.status_code is not None:
            metadata.append(f"status_code={self.status_code!r}")
        if self.retryable:
            metadata.append("retryable=True")
        suffix = f", {', '.join(metadata)}" if metadata else ""
        return f"{type(self).__name__}({self.safe_message!r}{suffix})"


class SharadarConfigurationError(SharadarError):
    """The client or request was configured incorrectly."""


class SharadarTransportError(SharadarError):
    """All attempts failed before a usable HTTP response arrived."""


class SharadarAuthenticationError(SharadarError):
    """The API key was absent, invalid, or otherwise rejected (HTTP 401)."""


class SharadarEntitlementError(SharadarError):
    """The account is not entitled to the requested table/history (HTTP 403)."""


class SharadarRateLimitError(SharadarError):
    """The server kept rate limiting after all configured retries."""


class SharadarResponseError(SharadarError):
    """A deterministic HTTP or protocol-level response was unusable."""


class SharadarDecodeError(SharadarResponseError):
    """A successful response could not be decoded as the requested format."""


class SharadarRedirectError(SharadarResponseError):
    """A bulk response did not contain a safe HTTPS download redirect."""
