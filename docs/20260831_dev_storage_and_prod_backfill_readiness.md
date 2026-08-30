# DEV storage and PROD backfill readiness

Date: 2026-08-31

## Outcome

The Sharadar vendor pipeline is now prepared up to, but not including, the
first `SHARADAR_PROD` full-history write.  A real paid-data canary is published
in Mongo `SHARADAR_DEV` and on the supercomputer-mounted NAS at:

```text
/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/dev
```

The production directory exists as an empty deployment target. Mongo database
`SHARADAR_PROD` has not been created and no production data download has run.

## Storage contract

```text
Sharadar REST / Bulk
  -> immutable content-addressed raw capture
  -> pinned schema v1 exact-header and type admission
  -> normalized Parquet on NAS
  -> normalized current collection in Mongo
  -> watermark
  -> immutable published run manifest (commit marker, written last)
```

The active registry covers the seven Fundamentals-plan tables and is based on
the official public PostgreSQL schemas dated 2026-08-18.  Registry resource
SHA-256:

```text
df600cbd1b10e7db6cb32f845aeb2cfd4cc66e4567b0a51f66a922e75a795455
```

Initial history uses Bulk. REST is reserved for bounded DEV canaries and daily
`lastupdated` deltas with an overlap window. Corporate/event tables without a
`lastupdated` clock are refreshed through periodic Bulk reconciliation.

## Real DEV evidence

| Table | Mongo collection | Rows |
|---|---|---:|
| descriptions | `normalized_descriptions_current` | 250 |
| tickers | `normalized_tickers_current` | 18 |
| fundamentals | `normalized_fundamentals_current` | 1,000 |
| daily | `normalized_daily_current` | 744 |
| actions | `normalized_actions_current` | 250 |
| events | `normalized_events_current` | 250 |
| sp500 | `normalized_sp500_current` | 250 |
| **Total** |  | **2,762** |

The fundamentals slice contains AAPL, JPM, MSFT, NVDA, UNH, and XOM.  Every
issuer has at least two distinct ARQ report periods; ARQ, ARY, and ART are all
present. This is a bounded engineering canary, not a research universe or a
factor artifact.

The read-only promotion report is stored at:

```text
/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/dev/readiness/latest.json
```

It reports `ready_for_prod_backfill=true`, `failed_checks=0`, and deliberately
keeps `prod_write_authorized=false`. Checks cover published manifests, row
counts, primary-key nulls/duplicates, schema fingerprints, PIT clock presence
and order, raw/Parquet checksums, deterministic replay, indexes, watermarks,
and the two-quarter ARQ canary.

## Live issues found and fixed

Two integration defects appeared only on the real deployment path:

1. The SMB NAS does not support hard links. Immutable publication now prefers a
   hard link where supported and uses a same-filesystem atomic replace fallback
   for explicit unsupported-filesystem errors. Existing content-addressed files
   are verified by size and SHA before replay.
2. PyMongo cannot encode `datetime.date`. Parquet retains Arrow `date32`, while
   Mongo converts dates to UTC-midnight BSON datetimes immediately before the
   write.

Both fixes were rerun through the real seven-table DEV materialization and the
readiness gate.

## PROD execution

The default command is a safe live-DEV recheck plus a dry plan:

```bash
venv/bin/python scripts/backfill_prod.py
```

It currently returns `status=READY` for all seven tables.  To start the actual
full-history Bulk backfill after William gives the final go-ahead:

```bash
venv/bin/python scripts/backfill_prod.py \
  --execute \
  --confirmation SHARADAR_PROD_WRITE \
  --production-confirmation BACKFILL_SHARADAR_PROD
```

The command re-runs the live DEV readiness gate before connecting to PROD.  It
downloads each table through a credential-free second hop, requires exact
versioned headers, streams CSV with bounded memory, writes Mongo in batches,
publishes Parquet and a final manifest, and can resume selected tables with
repeated `--table` options.

## Daily update after the baseline

The three `lastupdated` tables use REST deltas with a default three-day overlap:

```bash
venv/bin/python scripts/update_incremental.py \
  --deployment prod \
  --confirmation SHARADAR_PROD_WRITE \
  --production-confirmation BACKFILL_SHARADAR_PROD
```

The first successful bulk run records the maximum available `lastupdated` as
the starting watermark. Re-running the bulk command for one or more tables is
the reconciliation mechanism; content hashes and primary-key upserts make it
idempotent.

## Deferred tests

Large crash/concurrency/memory soaks, signed-URL-expiry reacquisition, and a
complete paid full-history acceptance run remain deferred until the actual PROD
backfill. The current milestone used the SDK suite, focused storage/readiness
tests, real paid REST canary, checksum verification, and one deterministic
replay per table.
