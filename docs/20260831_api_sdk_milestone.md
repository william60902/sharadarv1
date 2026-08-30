# Sharadar API SDK milestone

Date: 2026-08-31

## Outcome

The repository now has a reusable, credential-safe SDK for all 14 Sharadar table
routes.  The active Fundamentals subscription was exercised against all seven
entitled data routes: `descriptions`, `tickers`, `fundamentals`, `daily`,
`actions`, `events`, and `sp500`.  Their status and full-history bulk redirect
routes also passed without persisting or printing signed URLs.

The SDK provides validated typed queries, bounded pagination, retry and response
contracts, public schema access, and a streaming bulk downloader.  Bulk objects
are written as immutable content-addressed ZIPs with atomic manifests, SHA-256,
ZIP/CSV integrity checks, resource caps, and a credential-free second HTTP hop.

## Verification

- Offline SDK suite: 226 tests plus 45 subtests passed.
- Bounded local performance checks: 2 passed.
- Ruff, Python compilation, and `git diff --check` passed.
- Independent red-team gate: no remaining P0/P1 issue in the SDK scope.

The real bulk activation gate is intentionally stricter than the SDK default:
before a DEV or PROD bulk object is admitted, the pipeline must load a pinned,
versioned table schema and pass its exact ordered headers to `BulkDownloader`.

## Storage boundary

`SHARADAR_DEV` and `SHARADAR_PROD` own vendor capture and normalization only:
immutable raw objects, normalized vendor tables, schemas, ingestion manifests,
and watermarks.  Factor artifacts remain owned by the separate
`PDATA -> PSTAGE -> PMART` pipeline.

## Next promotion sequence

1. Pin the seven entitled table schemas.
2. Implement Mongo and Parquet storage with manifest-last publication.
3. Materialize a bounded real-data canary into `SHARADAR_DEV`.
4. Verify primary keys, PIT clocks, row counts, replay, checksums, and indexes.
5. Prepare but do not automatically start the `SHARADAR_PROD` full-history bulk
   backfill.

