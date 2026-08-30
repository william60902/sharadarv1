"""Reusable Sharadar API and ingestion SDK."""

from .auth import client_from_vault
from .bulk import BulkDownloader, BulkDownloadReceipt
from .catalog import (
    HistoryWindow,
    OutputFormat,
    Plan,
    SchemaDialect,
    SharadarTable,
    TableSpec,
    normalize_table,
    table_spec,
)
from .client import (
    BulkRedirect,
    FilterOperator,
    QueryFilter,
    QuerySpec,
    RetryPolicy,
    SharadarClient,
    SortDirection,
    SortSpec,
)
from .errors import SharadarError
from .routes import (
    Deployment,
    SharadarRoute,
    require_route_io,
    route_for,
)

__all__ = [
    "BulkDownloadReceipt",
    "BulkDownloader",
    "BulkRedirect",
    "Deployment",
    "FilterOperator",
    "HistoryWindow",
    "OutputFormat",
    "Plan",
    "QueryFilter",
    "QuerySpec",
    "RetryPolicy",
    "SchemaDialect",
    "SharadarClient",
    "SharadarError",
    "SharadarRoute",
    "SharadarTable",
    "SortDirection",
    "SortSpec",
    "TableSpec",
    "client_from_vault",
    "normalize_table",
    "require_route_io",
    "route_for",
    "table_spec",
]

__version__ = "0.1.0"
