#!/usr/bin/env python3
"""Emit the read-only SHARADAR_DEV promotion gate as machine-readable JSON."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sharadar_pipeline.readiness import (
    READINESS_FORMAT,
    gate_specs_from_registry,
    verify_dev_readiness,
)
from sharadar_pipeline.readiness_storage import DevStorageEvidenceSource
from sharadar_pipeline.runtime import connect_mongo_runtime
from sharadar_pipeline.schema_registry import load_schema_registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read SHARADAR_DEV manifests, artifacts and Mongo rows; never write PROD"
        )
    )
    parser.add_argument(
        "--compact", action="store_true", help="emit one-line JSON"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="also write the readiness JSON to this explicit local path",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime = None
    try:
        registry = load_schema_registry()
        runtime = connect_mongo_runtime("dev", write=False)
        source = DevStorageEvidenceSource(
            runtime.database, runtime.route.artifact_root
        )
        report = verify_dev_readiness(
            source, gate_specs_from_registry(registry), route=runtime.route
        )
        rendered = report.to_json(indent=None if args.compact else 2) + "\n"
        if args.output is not None:
            _write_report(args.output, rendered)
        sys.stdout.write(rendered)
        return 0 if report.ready_for_prod_backfill else 1
    except Exception as exc:  # noqa: BLE001 - sanitize every operational failure
        # Do not leak connection strings, signed URLs, filesystem contents, or
        # arbitrary adapter messages to a machine-consumed gate record.
        error = {
            "format": READINESS_FORMAT,
            "ready_for_prod_backfill": False,
            "prod_write_authorized": False,
            "error_type": type(exc).__name__,
            "error": "DEV readiness verification could not complete",
        }
        sys.stdout.write(json.dumps(error, sort_keys=True) + "\n")
        return 2
    finally:
        if runtime is not None:
            runtime.close()


def _write_report(path: Path, rendered: str) -> None:
    exact = path.expanduser().resolve()
    exact.parent.mkdir(parents=True, exist_ok=True)
    temporary = exact.with_name(f".{exact.name}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(exact)


if __name__ == "__main__":
    raise SystemExit(main())
