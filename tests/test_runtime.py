from __future__ import annotations

from typing import Any

import pytest

from sharadar_pipeline.routes import ProductionRouteLocked
from sharadar_pipeline.runtime import connect_mongo_runtime


class _Admin:
    def command(self, name: str) -> dict[str, float]:
        assert name == "ping"
        return {"ok": 1.0}


class _Client:
    def __init__(self) -> None:
        self.admin = _Admin()
        self.closed = False

    def __getitem__(self, name: str) -> str:
        return name

    def close(self) -> None:
        self.closed = True


class _Connector:
    profiles: list[str] = []

    def __init__(self, *, profile: str) -> None:
        self.profiles.append(profile)
        self.client = _Client()

    def get_client(self) -> _Client:
        return self.client


def test_dev_route_is_authorized_before_connector_creation() -> None:
    runtime = connect_mongo_runtime(
        "dev",
        write=True,
        confirmation="SHARADAR_DEV_WRITE",
        connector_factory=_Connector,
    )
    assert runtime.database == "SHARADAR_DEV"
    runtime.close()
    assert runtime.client.closed is True


def test_prod_rejection_occurs_before_secret_or_connector_io() -> None:
    calls: list[str] = []

    class GuardConnector:
        def __init__(self, *, profile: str) -> None:
            calls.append(profile)

    with pytest.raises(ProductionRouteLocked):
        connect_mongo_runtime(
            "prod",
            write=True,
            confirmation="SHARADAR_PROD_WRITE",
            connector_factory=GuardConnector,
        )
    assert calls == []


def test_read_route_uses_readonly_profile() -> None:
    _Connector.profiles.clear()
    runtime = connect_mongo_runtime(
        "dev",
        write=False,
        connector_factory=_Connector,
    )
    assert _Connector.profiles == ["readonly"]
    runtime.close()
