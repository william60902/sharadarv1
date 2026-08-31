#!/usr/bin/env python3
"""Verify the published SHARADAR_PROD baseline without scanning all rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.runtime import connect_mongo_runtime
from sharadar_pipeline.schema_registry import (
    FUNDAMENTALS_TABLES,
    load_schema_registry,
)
from sharadar_pipeline.storage.artifacts import resolve_registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read PROD manifests, artifact metadata and Mongo collection stats. "
            "Use --rehash only for an explicit full artifact-integrity audit."
        )
    )
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--rehash",
        action="store_true",
        help="re-read every raw and Parquet artifact and verify SHA-256",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_schema_registry()
    runtime = connect_mongo_runtime("prod", write=False)
    try:
        root = runtime.route.artifact_root.resolve()
        expected = [table.value for table in FUNDAMENTALS_TABLES]
        actual = sorted(runtime.database.list_collection_names())
        collection_set_ok = set(actual) == set(expected)
        results: list[dict[str, Any]] = []

        for table in expected:
            schema = registry.table(table)
            storage_schema = resolve_registry(registry, table)
            watermark = _read_json(root / "watermarks" / f"{table}.json")
            run_id = _required_string(watermark, "run_id")
            manifest_path = root / "runs" / table / f"{run_id}.json"
            manifest = _read_json(manifest_path)
            raw = _required_mapping(manifest, "raw_capture")
            parquet = _required_mapping(manifest, "parquet")
            raw_path = _artifact_path(root, raw)
            parquet_path = _artifact_path(root, parquet)
            mongo_count = int(runtime.database.command("collStats", table)["count"])
            indexes = runtime.database[table].index_information()
            expected_pk = tuple(schema.primary_key)

            checks = {
                "published_manifest": (
                    manifest.get("published") is True
                    and manifest.get("status") == "published"
                    and manifest.get("run_id") == run_id
                    and manifest.get("table") == table
                ),
                "schema_fingerprint": (
                    manifest.get("schema_fingerprint") == storage_schema.fingerprint
                ),
                "mongo_matches_manifest": mongo_count == manifest.get("row_count"),
                "parquet_matches_manifest": (
                    parquet.get("row_count") == manifest.get("row_count")
                ),
                "raw_size": _size_matches(raw_path, raw),
                "parquet_size": _size_matches(parquet_path, parquet),
                "primary_key_index": _has_unique_pk(indexes, expected_pk),
                "watermark": (
                    watermark.get("run_id") == run_id
                    and watermark.get("table") == table
                    and watermark.get("schema_fingerprint")
                    == manifest.get("schema_fingerprint")
                ),
            }
            if args.rehash:
                checks["raw_sha256"] = _sha256(raw_path) == raw.get("sha256")
                checks["parquet_sha256"] = (
                    _sha256(parquet_path) == parquet.get("sha256")
                )
            results.append(
                {
                    "table": table,
                    "rows": mongo_count,
                    "run_id": run_id,
                    "checks": checks,
                    "passed": all(checks.values()),
                }
            )

        passed = collection_set_ok and all(item["passed"] for item in results)
        report = {
            "format": "sharadar.prod-baseline-readback/v1",
            "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "status": "PASS" if passed else "FAIL",
            "database": runtime.route.database_name,
            "artifact_root": str(root),
            "collection_set_ok": collection_set_ok,
            "collections": actual,
            "rehash": args.rehash,
            "total_rows": sum(item["rows"] for item in results),
            "tables": results,
        }
        rendered = json.dumps(
            report,
            indent=None if args.compact else 2,
            sort_keys=True,
            default=str,
        ) + "\n"
        if args.output is not None:
            _write_report(args.output, rendered)
        sys.stdout.write(rendered)
        return 0 if passed else 1
    finally:
        runtime.close()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"metadata is not an object: {path.name}")
    return payload


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing string metadata: {key}")
    return value


def _required_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"missing object metadata: {key}")
    return value


def _artifact_path(root: Path, receipt: dict[str, Any]) -> Path:
    declared = _required_string(receipt, "artifact_path")
    path = Path(declared).expanduser().resolve()
    if not path.is_relative_to(root):
        raise ValueError("artifact path escapes SHARADAR_PROD root")
    return path


def _size_matches(path: Path, receipt: dict[str, Any]) -> bool:
    expected = receipt.get("byte_count")
    return type(expected) is int and path.is_file() and path.stat().st_size == expected


def _has_unique_pk(indexes: dict[str, Any], expected_pk: tuple[str, ...]) -> bool:
    for metadata in indexes.values():
        if metadata.get("unique") is not True:
            continue
        keys = tuple(field for field, _direction in metadata.get("key", []))
        if keys == expected_pk:
            return True
    return False


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_report(path: Path, rendered: str) -> None:
    exact = path.expanduser().resolve()
    exact.parent.mkdir(parents=True, exist_ok=True)
    temporary = exact.with_name(f".{exact.name}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(exact)


if __name__ == "__main__":
    raise SystemExit(main())
