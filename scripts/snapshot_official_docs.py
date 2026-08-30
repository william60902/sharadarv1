#!/usr/bin/env python3
"""Capture a reproducible snapshot of Sharadar's public documentation.

This script deliberately stays on ``sharadar.com`` and downloads only the
public documentation URLs declared by the official sitemap, plus the public
robots, sitemap and llms index files. It never reads an API key and never calls
the licensed data API.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from pathlib import Path


SITE_ORIGIN = "https://sharadar.com"
ROOT_SITEMAP_URL = f"{SITE_ORIGIN}/sitemap.xml"
PUBLIC_REFERENCE_URLS = (
    f"{SITE_ORIGIN}/robots.txt",
    ROOT_SITEMAP_URL,
    f"{SITE_ORIGIN}/llms.txt",
)
USER_AGENT = "Medina-SharadarV1-Docs/0.1 (+personal research reference)"


def _fetch(url: str, timeout: float) -> tuple[bytes, dict[str, str], str, int]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        headers = {key.lower(): value for key, value in response.headers.items()}
        return body, headers, response.geturl(), response.status


def _assert_public_sharadar_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "sharadar.com":
        raise ValueError(f"refusing non-Sharadar URL: {url}")
    if parsed.query or parsed.fragment:
        raise ValueError(f"refusing URL with query or fragment: {url}")
    allowed = parsed.path in {"/robots.txt", "/sitemap.xml", "/llms.txt"}
    allowed = allowed or parsed.path.startswith("/sitemap-")
    allowed = allowed or parsed.path == "/docs" or parsed.path.startswith("/docs/")
    if not allowed:
        raise ValueError(f"refusing non-documentation URL: {url}")


def _sitemap_locations(xml_bytes: bytes) -> list[str]:
    root = ET.fromstring(xml_bytes)
    return [
        element.text.strip()
        for element in root.iter()
        if element.tag.endswith("loc") and element.text
    ]


def _local_name(url: str, content_type: str) -> str:
    path = urllib.parse.urlparse(url).path
    if path == "/robots.txt":
        return "robots.txt"
    if path == "/llms.txt":
        return "llms.txt"
    if path.endswith(".xml"):
        return path.removeprefix("/").replace("/", "--")
    slug = path.removeprefix("/").replace("/", "--") or "index"
    suffix = ".html" if "html" in content_type else ".bin"
    # Case-only aliases such as SF1/sf1 collide on common macOS filesystems.
    identity = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]
    return f"{slug}--{identity}{suffix}"


def capture_snapshot(
    output_root: Path,
    *,
    snapshot_date: str,
    timeout: float,
    delay: float,
) -> Path:
    snapshot_dir = output_root / "snapshots" / snapshot_date
    raw_dir = snapshot_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    bodies: dict[str, tuple[bytes, dict[str, str], str, int]] = {}
    for url in PUBLIC_REFERENCE_URLS:
        _assert_public_sharadar_url(url)
        bodies[url] = _fetch(url, timeout)
        time.sleep(delay)

    root_sitemap = bodies[ROOT_SITEMAP_URL][0]
    child_sitemaps = sorted(set(_sitemap_locations(root_sitemap)))
    for url in child_sitemaps:
        _assert_public_sharadar_url(url)
        bodies[url] = _fetch(url, timeout)
        time.sleep(delay)

    docs_urls: set[str] = set()
    for url in child_sitemaps:
        for location in _sitemap_locations(bodies[url][0]):
            parsed = urllib.parse.urlparse(location)
            if parsed.scheme == "https" and parsed.netloc == "sharadar.com":
                if parsed.path == "/docs" or (
                    parsed.path.startswith("/docs/") and not parsed.query
                ):
                    docs_urls.add(location)

    for url in sorted(docs_urls):
        _assert_public_sharadar_url(url)
        bodies[url] = _fetch(url, timeout)
        time.sleep(delay)

    captured_at = datetime.now(UTC).isoformat()
    records = []
    for url in sorted(bodies):
        body, headers, final_url, status = bodies[url]
        content_type = headers.get("content-type", "application/octet-stream")
        filename = _local_name(url, content_type)
        path = raw_dir / filename
        path.write_bytes(body)
        records.append(
            {
                "source_url": url,
                "final_url": final_url,
                "local_path": str(path.relative_to(snapshot_dir)),
                "http_status": status,
                "content_type": content_type,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "etag": headers.get("etag"),
                "last_modified": headers.get("last-modified"),
                "captured_at_utc": captured_at,
            }
        )

    manifest = {
        "schema_version": 1,
        "source_origin": SITE_ORIGIN,
        "snapshot_date": snapshot_date,
        "captured_at_utc": captured_at,
        "scope": "public documentation only; no API key and no licensed data API",
        "document_count": len(docs_urls),
        "object_count": len(records),
        "objects": records,
    }
    manifest_path = snapshot_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (snapshot_dir / "SOURCE_URLS.txt").write_text(
        "\n".join(record["source_url"] for record in records) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Snapshot Sharadar public documentation declared by its sitemap."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("docs/sharadar_official"),
    )
    parser.add_argument("--snapshot-date", default=date.today().isoformat())
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--delay", type=float, default=0.10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = capture_snapshot(
        args.output_root,
        snapshot_date=args.snapshot_date,
        timeout=args.timeout,
        delay=args.delay,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    print(
        f"captured {manifest['document_count']} docs / "
        f"{manifest['object_count']} objects -> {manifest_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
