# SharadarV1 bootstrap and official documentation checkpoint

Date: 2026-08-31

## Outcome

The repository baseline, Vault credential path, paid API entitlement and public
official-documentation reference are now operational.

- Repository: `/Users/chouwilliam/Medina/sharadarv1`
- Python environment: repository-local `venv`
- Vault secret name: `sharadar_api_key` in `VAULT_PROD`
- The secret value is never stored in this repository, a command-line argument,
  a URL, a log or a documentation artifact.
- The API client sends the credential in the `x-api-key` header.

## Paid API entitlement evidence

The acceptance check deliberately uses `PLTR`, not the documented AAPL/free
sample path.

| Check | Result |
| --- | --- |
| `fundamentals`, one `PLTR` row | HTTP 200; requested ticker matched |
| `daily`, one `PLTR` row | HTTP 200; requested ticker matched |
| `tickers`, one `PLTR` row | HTTP 200; requested ticker matched |
| `fundamentals?years=full` | HTTP 302; signed download host present |

The bulk check stops at the redirect and does not download the full-history zip.
This proves the active Fundamentals Full History subscription supports the three
REST tables and the full-history bulk route without consuming a large artifact.

Re-run the safe check:

```bash
cd /Users/chouwilliam/Medina/sharadarv1
venv/bin/python scripts/check_api_access.py
```

The command prints only table names, status codes, row counts, ticker-match
booleans and redirect availability. It does not print response values, the API
key or the signed download URL.

## Official documentation capture

Three parallel reviews produced:

- `docs/sharadar_official/00_SOURCE_INDEX.md`: official page inventory,
  sitemap/alias behavior and reproducibility notes;
- `docs/sharadar_official/10_FUNDAMENTALS_AND_TABLES.md`: SF1/PIT dimensions,
  core supporting tables and identity/date semantics;
- `docs/sharadar_official/20_API_AND_INGESTION_RUNBOOK.md`: authentication,
  bulk bootstrap, incremental updates, watermarking and failure handling.

The public documentation snapshot at
`docs/sharadar_official/snapshots/2026-08-31` contains all 36 `/docs` URLs
declared by the official sitemap plus `robots.txt`, `sitemap.xml`,
`sitemap-0.xml` and `llms.txt`. Its manifest records source URLs, status,
content type, byte count, retrieval time and SHA-256 for all 40 objects.

Refresh it with:

```bash
cd /Users/chouwilliam/Medina/sharadarv1
venv/bin/python scripts/snapshot_official_docs.py
```

This capture lane is public documentation only. It never reads the Vault and
never calls Sharadar's licensed data API.

## Decisions carried into development

### Environment routes

The Mongo namespaces are fixed as:

| Deployment | Mongo database | Current write authority |
| --- | --- | --- |
| DEV | `SHARADAR_DEV` | active development/backfill target |
| PROD | `SHARADAR_PROD` | fully routed in code; locked until explicit promotion |

Environment selection must resolve the exact Mongo database, immutable artifact
root and confirmation policy before creating a connector. Business logic never
constructs a database name by concatenating arbitrary user input. This follows
Pentagon's `FactorPipelineRoute` separation and Brain Studio's explicit
`DB_DEV`/`DB_PROD` consumer boundary.

### Storage and publication boundary

```text
Sharadar API / full-history bulk
          |
          v
immutable ZIP/CSV capture + SHA-256 + capture receipt
          |
          v
typed, partitioned normalized Parquet + table-build manifest
          |
          +--> SHARADAR_DEV / SHARADAR_PROD control plane
          |      schemas, capture_runs, table_builds, watermarks,
          |      identity/action indexes and bounded query projections
          |
          v
Foundry exact build reference -> PDATA -> PSTAGE -> PMART
```

`SHARADAR_*` is the vendor-ingestion boundary. It preserves and normalizes
vendor facts but does not calculate factor ranks, z-scores or composites.
Foundry remains the owner of factor measurements and downstream transforms.
Large historical numeric tables are canonical Parquet artifacts; Mongo is the
control plane and bounded query/index surface, not a duplicate unversioned data
lake.

### PIT and update policy

1. Historical factor inputs use `ARQ`, `ARY` and `ART`. `MRQ`, `MRY` and `MRT`
   are retained separately and must not overwrite As-Reported history.
2. AR `date` is the conservative filing-date PIT availability clock;
   `reportperiod`, `calendardate` and `lastupdated` have different meanings.
3. `permaticker` is the stable issuer identity; ticker changes and corporate
   actions retain their own lineage.
4. Initial history comes from immutable `years=full` bulk capture. Daily work
   uses a configurable overlapping `lastupdated` window and advances its
   watermark only after complete durable publication.
5. Today’s tickers metadata is a snapshot, so current sector/industry values
   are not silently treated as historical PIT classifications.
6. Raw vendor downloads, CSV/ZIP/Parquet data and local artifacts remain outside
   Git; only code, manifests without credentials, public docs references and
   Medina-authored notes are versioned.
7. Backfill and daily update use the same parser/schema/manifest path. Backfill
   begins from `years=full`; daily update consumes an overlapping `lastupdated`
   window and creates new immutable partitions/builds instead of mutating raw
   captures in place.

## Next milestone

Implement the full-history bootstrap lane: public schema capture, `status=True`
preflight, controlled 302 download, immutable zip manifest, ZIP/CSV validation
and normalized raw Parquet. Only the minimal correctness checks in the API
runbook are required for this first release.
