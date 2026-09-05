"""Route-first runtime connectors for Sharadar Mongo storage."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .routes import SharadarRoute, require_route_io, route_for

import sys

MEDINA_ROOT = Path(__file__).resolve().parents[3]
if str(MEDINA_ROOT) not in sys.path:
    sys.path.insert(0, str(MEDINA_ROOT))

from pm.pkg.mongo_connector import (  # noqa: E402
    MongoConnector,
    PIPELINE_RW_PROFILE,
    READONLY_PROFILE,
)


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


def connect_mongo_runtime(
    deployment: str,
    *,
    write: bool,
    confirmation: str | None = None,
    production_confirmation: str | None = None,
    connector_factory: Callable[..., Any] = MongoConnector,
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
    profile = PIPELINE_RW_PROFILE if write else READONLY_PROFILE
    client = connector_factory(profile=profile).get_client()
    try:
        client.admin.command("ping")
        database = client[route.database_name]
    except Exception:
        close = getattr(client, "close", None)
        if callable(close):
            close()
        raise
    return MongoRuntime(route=route, client=client, database=database)
