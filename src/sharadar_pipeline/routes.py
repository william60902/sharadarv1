"""Fail-closed DEV/PROD routing for Sharadar ingestion state."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class RouteError(RuntimeError):
    pass


class ProductionRouteLocked(RouteError):
    pass


PRODUCTION_BACKFILL_CONFIRMATION = "BACKFILL_SHARADAR_PROD"


class Deployment(StrEnum):
    DEV = "dev"
    PROD = "prod"


@dataclass(frozen=True, slots=True)
class SharadarRoute:
    deployment: Deployment
    database_name: str
    artifact_root: Path
    write_confirmation: str
    live_write_authorized: bool


_ROUTES = {
    Deployment.DEV: SharadarRoute(
        deployment=Deployment.DEV,
        database_name="SHARADAR_DEV",
        artifact_root=Path(
            "/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/dev"
        ),
        write_confirmation="SHARADAR_DEV_WRITE",
        live_write_authorized=True,
    ),
    Deployment.PROD: SharadarRoute(
        deployment=Deployment.PROD,
        database_name="SHARADAR_PROD",
        artifact_root=Path(
            "/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/prod"
        ),
        write_confirmation="SHARADAR_PROD_WRITE",
        live_write_authorized=False,
    ),
}


def route_for(deployment: Deployment | str) -> SharadarRoute:
    try:
        exact = Deployment(deployment)
    except (TypeError, ValueError):
        raise RouteError(f"unknown Sharadar deployment: {deployment!r}") from None
    return _ROUTES[exact]


def require_route_io(
    route: SharadarRoute,
    *,
    database_name: str,
    write: bool,
    confirmation: str | None = None,
    production_confirmation: str | None = None,
) -> SharadarRoute:
    """Authorize an exact resolved route before connector/filesystem creation."""

    if type(route) is not SharadarRoute:
        raise RouteError("invalid Sharadar route")
    canonical = _ROUTES.get(route.deployment)
    if canonical is None or route is not canonical:
        raise RouteError("Sharadar route must come from route_for")
    if database_name != route.database_name:
        raise RouteError("database name does not match resolved Sharadar route")
    if (
        write
        and route.deployment is Deployment.PROD
        and production_confirmation != PRODUCTION_BACKFILL_CONFIRMATION
    ):
        raise ProductionRouteLocked(
            "SHARADAR_PROD requires the explicit backfill promotion confirmation"
        )
    if write and not route.live_write_authorized and route.deployment is not Deployment.PROD:
        raise RouteError("Sharadar route does not authorize live writes")
    if write and confirmation != route.write_confirmation:
        raise RouteError("exact Sharadar write confirmation is required")
    return route
