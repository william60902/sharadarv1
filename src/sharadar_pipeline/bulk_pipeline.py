"""O(n), bounded-memory bulk ZIP to Parquet and Mongo orchestration."""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bulk import BulkDownloader, BulkDownloadReceipt
from .catalog import HistoryWindow, SharadarTable, normalize_table
from .storage import RawCaptureReceipt, StorageRunReceipt, VendorStorageEngine


@dataclass(frozen=True, slots=True)
class BulkPipelineReceipt:
    download: BulkDownloadReceipt
    storage: StorageRunReceipt


def ingest_bulk_table(
    *,
    client: Any,
    table: SharadarTable | str,
    history: HistoryWindow | str,
    registry: Any,
    storage_engine: VendorStorageEngine,
    artifact_root: Path,
) -> BulkPipelineReceipt:
    """Download, exact-schema validate, stream, and publish one bulk table."""

    exact_table = normalize_table(table)
    schema = registry.table(exact_table)
    downloader = BulkDownloader(
        client,
        expected_headers={exact_table: schema.ordered_headers},
    )
    download = downloader.download(
        exact_table,
        history,
        artifact_root / "raw" / "bulk" / exact_table.value,
    )
    if len(download.csv_members) != 1:
        raise RuntimeError("validated Sharadar bulk object must contain one CSV")
    raw = RawCaptureReceipt(
        table=exact_table.value,
        sha256=download.sha256,
        byte_count=download.received_bytes,
        artifact_path=download.artifact_path,
        replayed=download.replayed,
    )
    watermark = {
        "value": f"bulk:{HistoryWindow(history).value}:{download.sha256}",
    }
    rows = _track_source_watermark(
        iter_bulk_csv_rows(
            download.artifact_path,
            member=download.csv_members[0],
            expected_headers=schema.ordered_headers,
        ),
        field=(
            "lastupdated"
            if "lastupdated" in schema.ordered_headers
            else "date"
            if "date" in schema.ordered_headers
            else None
        ),
        watermark=watermark,
    )
    storage = storage_engine.ingest_rows(
        exact_table.value,
        raw,
        rows,
        registry,
        source_watermark=watermark,
        pipeline_version="bulk-v1",
    )
    return BulkPipelineReceipt(download=download, storage=storage)


def _track_source_watermark(
    rows: Iterator[Mapping[str, str | None]],
    *,
    field: str | None,
    watermark: dict[str, str],
) -> Iterator[Mapping[str, str | None]]:
    maximum: str | None = None
    for row in rows:
        value = row.get(field) if field is not None else None
        if value and (maximum is None or value > maximum):
            maximum = value
            watermark["value"] = value
        yield row


def iter_bulk_csv_rows(
    archive: Path,
    *,
    member: str,
    expected_headers: tuple[str, ...],
) -> Iterator[Mapping[str, str | None]]:
    """Yield one CSV row at a time; ZIP validation already happened upstream."""

    with (
        zipfile.ZipFile(archive, "r") as bundle,
        bundle.open(member, "r") as binary,
        io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as text,
    ):
        reader = csv.DictReader(text)
        if tuple(reader.fieldnames or ()) != expected_headers:
            raise RuntimeError("bulk CSV headers differ from pinned schema")
        for row in reader:
            if None in row:
                raise RuntimeError("bulk CSV row has more fields than its header")
            yield {key: value for key, value in row.items()}
