"""Medina Vault adapter kept separate from the transport implementation."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .client import SharadarClient
from .errors import SharadarConfigurationError

SecretGetter = Callable[..., object]


def _workspace_secret_getter() -> SecretGetter:
    try:
        from hub.vault import get_secret

        return get_secret
    except ModuleNotFoundError:
        # Editable Medina checkouts keep hub beside sharadarv1. This fallback
        # adds only the workspace root; it never reads a secret or config file.
        workspace_root = Path(__file__).resolve().parents[3]
        if (workspace_root / "hub" / "vault").is_dir():
            root_text = str(workspace_root)
            if root_text not in sys.path:
                sys.path.insert(0, root_text)
            try:
                from hub.vault import get_secret

                return get_secret
            except ModuleNotFoundError:
                pass
    raise SharadarConfigurationError(
        "Medina Hub Vault is not importable; inject secret_getter explicitly"
    ) from None


def client_from_vault(
    *,
    secret_name: str = "sharadar_api_key",
    env: str = "prod",
    secret_getter: SecretGetter | None = None,
    **client_kwargs: Any,
) -> SharadarClient:
    """Build a client without placing the API key in config, argv, or URLs."""

    if secret_getter is None:
        secret_getter = _workspace_secret_getter()
    api_key = secret_getter(secret_name, env=env)
    return SharadarClient(api_key, **client_kwargs)
