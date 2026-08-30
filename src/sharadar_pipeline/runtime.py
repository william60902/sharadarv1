"""Route-first runtime connectors for Sharadar Mongo storage."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pymongo import MongoClient

from .routes import SharadarRoute, require_route_io, route_for


@dataclass(frozen=True, slots=True)
class MongoRuntime:
    """A resolved database handle whose route was authorized before I/O."""

    route: SharadarRoute
    client: Any
    database: Any

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if callable(close):
            close()


def _workspace_mongo_uri_getter() -> str:
    """Read the Medina Mongo bootstrap without exposing it to argv or logs."""

    workspace_root = Path(__file__).resolve().parents[3]
    root_text = str(workspace_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)
    try:
        from hub.vault.core import MONGO_URI_FILE, _read_bootstrap_file
    except ModuleNotFoundError:
        raise RuntimeError("Medina Hub Vault bootstrap is not importable") from None
    return _read_bootstrap_file(MONGO_URI_FILE)


def connect_mongo_runtime(
    deployment: str,
    *,
    write: bool,
    confirmation: str | None = None,
    production_confirmation: str | None = None,
    uri_getter: Callable[[], str] | None = None,
    client_factory: Callable[..., Any] = MongoClient,
) -> MongoRuntime:
    """Resolve and authorize the exact route before reading Mongo credentials."""

    route = route_for(deployment)
    require_route_io(
        route,
        database_name=route.database_name,
        write=write,
        confirmation=confirmation,
        production_confirmation=production_confirmation,
    )
    getter = uri_getter or _workspace_mongo_uri_getter
    uri = getter()
    client = client_factory(
        uri,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=120_000,
        tz_aware=True,
        appname="medina-sharadar",
    )
    try:
        client.admin.command("ping")
        database = client[route.database_name]
    except Exception:
        close = getattr(client, "close", None)
        if callable(close):
            close()
        raise
    return MongoRuntime(route=route, client=client, database=database)
