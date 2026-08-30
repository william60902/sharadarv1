"""Reusable Sharadar API and ingestion SDK."""

from .auth import client_from_vault
from .bulk import BulkDownloader, BulkDownloadReceipt
from .bulk_pipeline import BulkPipelineReceipt, ingest_bulk_table
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
    PRODUCTION_BACKFILL_CONFIRMATION,
    Deployment,
    SharadarRoute,
    require_route_io,
    route_for,
)
from .runtime import MongoRuntime, connect_mongo_runtime
from .schema_registry import (
    SchemaRegistry,
    TableSchema,
    expected_headers,
    get_table_schema,
    load_schema_registry,
)
from .storage import ArtifactStore, MongoCurrentStore, VendorStorageEngine

__all__ = [
    "PRODUCTION_BACKFILL_CONFIRMATION",
    "ArtifactStore",
    "BulkDownloadReceipt",
    "BulkDownloader",
    "BulkPipelineReceipt",
    "BulkRedirect",
    "Deployment",
    "FilterOperator",
    "HistoryWindow",
    "MongoCurrentStore",
    "MongoRuntime",
    "OutputFormat",
    "Plan",
    "QueryFilter",
    "QuerySpec",
    "RetryPolicy",
    "SchemaDialect",
    "SchemaRegistry",
    "SharadarClient",
    "SharadarError",
    "SharadarRoute",
    "SharadarTable",
    "SortDirection",
    "SortSpec",
    "TableSchema",
    "TableSpec",
    "VendorStorageEngine",
    "client_from_vault",
    "connect_mongo_runtime",
    "expected_headers",
    "get_table_schema",
    "ingest_bulk_table",
    "load_schema_registry",
    "normalize_table",
    "require_route_io",
    "route_for",
    "table_spec",
]

__version__ = "0.2.0"
