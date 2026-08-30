# Sharadar official documentation reference

This directory separates official source snapshots from Medina engineering
notes.

- `snapshots/<date>/raw/` contains public pages declared by Sharadar's official
  sitemap, together with `robots.txt`, the sitemap files and `llms.txt`.
- Each snapshot has a `manifest.json` containing the source URL, retrieval time,
  HTTP status, content type, byte count and SHA-256 digest.
- Numbered Markdown files in this directory are Medina-authored summaries and
  runbooks. They cite the live official pages rather than pretending to be the
  source of truth.

The snapshot tool never reads `sharadar_api_key` and never calls the licensed
data API. It is intentionally restricted to public documentation under
`https://sharadar.com/docs/` and official discovery files.

Refresh the reference snapshot:

```bash
cd /Users/chouwilliam/Medina/sharadarv1
venv/bin/python scripts/snapshot_official_docs.py
```

Live documentation remains authoritative:
<https://sharadar.com/docs/intro>.
