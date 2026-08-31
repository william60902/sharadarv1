import sys
from pathlib import Path

import pytest

from sharadar_pipeline.catalog import (
    TABLE_SPECS,
    HistoryWindow,
    Plan,
    SharadarTable,
    normalize_table,
    table_spec,
)
from sharadar_pipeline.routes import (
    PRODUCTION_BACKFILL_CONFIRMATION,
    Deployment,
    ProductionRouteLocked,
    RouteError,
    SharadarRoute,
    require_route_io,
    route_for,
)


def test_catalog_covers_every_documented_table_once():
    assert len(SharadarTable) == 14
    assert set(TABLE_SPECS) == set(SharadarTable)
    assert all(spec.table is table for table, spec in TABLE_SPECS.items())


@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        ("SF1", SharadarTable.FUNDAMENTALS),
        ("sep", SharadarTable.STOCKS),
        ("SFP", SharadarTable.FUNDS),
        ("sf2", SharadarTable.INSIDERS),
        ("SF3", SharadarTable.HOLDINGS),
        ("sf3a", SharadarTable.HOLDINGS_TICKER),
        ("SF3B", SharadarTable.HOLDINGS_INVESTOR),
        ("holdings-ticker", SharadarTable.HOLDINGS_TICKER),
        ("indicator", SharadarTable.DESCRIPTIONS),
        ("INDICATORS", SharadarTable.DESCRIPTIONS),
    ],
)
def test_legacy_and_path_aliases_normalize(alias, expected):
    assert normalize_table(alias) is expected


@pytest.mark.parametrize("bad", [None, "", "unknown", 7])
def test_unknown_or_empty_table_fails_closed(bad):
    with pytest.raises(ValueError):
        normalize_table(bad)


def test_fundamentals_plan_entitlement_is_explicit():
    expected = {
        SharadarTable.DESCRIPTIONS,
        SharadarTable.TICKERS,
        SharadarTable.FUNDAMENTALS,
        SharadarTable.DAILY,
        SharadarTable.ACTIONS,
        SharadarTable.EVENTS,
        SharadarTable.SP500,
    }
    actual = {
        table for table, spec in TABLE_SPECS.items() if Plan.FUNDAMENTALS in spec.plans
    }
    assert actual == expected
    assert Plan.FUNDAMENTALS not in table_spec("stocks").plans


def test_history_window_wire_values_are_exact():
    assert [window.value for window in HistoryWindow] == ["5", "10", "full"]


def test_routes_bind_exact_database_and_artifact_root():
    dev = route_for("dev")
    prod = route_for(Deployment.PROD)
    assert dev.database_name == "SHARADAR_DEV"
    base = (
        Path("/Volumes/Medina_US_Equity/sharadar")
        if sys.platform == "darwin"
        else Path("/mnt/nas/Medina_US_Equity/sharadar")
    )
    assert dev.artifact_root == base / "dev"
    assert prod.database_name == "SHARADAR_PROD"
    assert prod.artifact_root == base / "prod"


def test_dev_write_requires_exact_confirmation():
    dev = route_for("dev")
    with pytest.raises(RouteError):
        require_route_io(dev, database_name="SHARADAR_DEV", write=True)
    assert require_route_io(
        dev,
        database_name="SHARADAR_DEV",
        write=True,
        confirmation="SHARADAR_DEV_WRITE",
    ) is dev


def test_cross_database_and_prod_write_fail_before_io():
    dev = route_for("dev")
    with pytest.raises(RouteError):
        require_route_io(
            dev,
            database_name="SHARADAR_PROD",
            write=False,
        )

    prod = route_for("prod")
    with pytest.raises(ProductionRouteLocked):
        require_route_io(
            prod,
            database_name="SHARADAR_PROD",
            write=True,
            confirmation="SHARADAR_PROD_WRITE",
        )

    assert require_route_io(
        prod,
        database_name="SHARADAR_PROD",
        write=True,
        confirmation="SHARADAR_PROD_WRITE",
        production_confirmation=PRODUCTION_BACKFILL_CONFIRMATION,
    ) is prod


def test_prod_route_remains_readable_for_future_consumers():
    prod = route_for("prod")
    assert require_route_io(prod, database_name="SHARADAR_PROD", write=False) is prod


def test_forged_route_cannot_cross_database_or_artifact_boundaries():
    forged = SharadarRoute(
        deployment=Deployment.DEV,
        database_name="SHARADAR_PROD",
        artifact_root=route_for("prod").artifact_root,
        write_confirmation="SHARADAR_DEV_WRITE",
        live_write_authorized=True,
    )
    with pytest.raises(RouteError, match="route_for"):
        require_route_io(
            forged,
            database_name="SHARADAR_PROD",
            write=True,
            confirmation="SHARADAR_DEV_WRITE",
        )
