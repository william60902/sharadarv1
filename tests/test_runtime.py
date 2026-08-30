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
    def __init__(self, uri: str, **kwargs: Any) -> None:
        assert uri == "mongodb://runtime-test"
        assert kwargs["tz_aware"] is True
        self.admin = _Admin()
        self.closed = False

    def __getitem__(self, name: str) -> str:
        return name

    def close(self) -> None:
        self.closed = True


def test_dev_route_is_authorized_before_connector_creation() -> None:
    runtime = connect_mongo_runtime(
        "dev",
        write=True,
        confirmation="SHARADAR_DEV_WRITE",
        uri_getter=lambda: "mongodb://runtime-test",
        client_factory=_Client,
    )
    assert runtime.database == "SHARADAR_DEV"
    runtime.close()
    assert runtime.client.closed is True


def test_prod_rejection_occurs_before_secret_or_connector_io() -> None:
    calls: list[str] = []

    def getter() -> str:
        calls.append("secret")
        return "mongodb://runtime-test"

    with pytest.raises(ProductionRouteLocked):
        connect_mongo_runtime(
            "prod",
            write=True,
            confirmation="SHARADAR_PROD_WRITE",
            uri_getter=getter,
            client_factory=_Client,
        )
    assert calls == []
