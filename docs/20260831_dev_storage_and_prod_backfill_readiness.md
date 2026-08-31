# DEV storage and PROD full-history baseline

Date: 2026-08-31

## Outcome

The Sharadar vendor pipeline now has both a paid-data DEV canary and a completed
`SHARADAR_PROD` full-history baseline.  DEV is published in Mongo
`SHARADAR_DEV` and on the supercomputer-mounted NAS at:

```text
/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/dev
```

PROD is published in Mongo `SHARADAR_PROD` and at:

```text
/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/prod
```

## Storage contract

```text
Sharadar REST / Bulk
  -> immutable content-addressed raw capture
  -> pinned schema v1 exact-header and type admission
  -> schema-admitted Parquet on NAS
  -> schema-admitted query collection in Mongo
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
| descriptions | `descriptions` | 250 |
| tickers | `tickers` | 18 |
| fundamentals | `fundamentals` | 1,000 |
| daily | `daily` | 744 |
| actions | `actions` | 250 |
| events | `events` | 250 |
| sp500 | `sp500` | 250 |
| **Total** |  | **2,762** |

The fundamentals slice contains AAPL, JPM, MSFT, NVDA, UNH, and XOM.  Every
issuer has at least two distinct ARQ report periods; ARQ, ARY, and ART are all
present. This is a bounded engineering canary, not a research universe or a
factor artifact.

Before the first PROD backfill, Mongo collections were intentionally simplified
from `normalized_<table>_current` to the public Sharadar table names.  The word
`normalized` described schema/type admission, not factor z-scoring or ranking,
and was therefore misleading inside a quant platform.  Vendor raw captures stay
immutable on NAS; Mongo collection naming does not change lineage or replay.

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

## PROD full-history result

The first full-history Bulk baseline completed on 2026-08-31:

| Table | Mongo collection | Rows |
|---|---|---:|
| descriptions | `descriptions` | 382 |
| tickers | `tickers` | 74,078 |
| fundamentals | `fundamentals` | 3,216,147 |
| daily | `daily` | 40,083,673 |
| actions | `actions` | 688,880 |
| events | `events` | 2,585,951 |
| sp500 | `sp500` | 59,674 |
| **Total** |  | **46,708,785** |

The full `fundamentals` dimension distribution confirms that As-Reported and
Most-Recent lanes remain independently addressable:

| Dimension | Rows |
|---|---:|
| ARQ | 681,430 |
| ART | 688,153 |
| ARY | 186,623 |
| MRQ | 718,807 |
| MRT | 737,798 |
| MRY | 203,336 |

Factor PDATA must explicitly select `ARQ` / `ARY` / `ART` and use AR `date` as
the conservative filing-date availability clock. MR dimensions are retained for
comparison and restatement-aware research, not silently mixed into PIT signals.

The read-only baseline report is stored at:

```text
/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/prod/readiness/latest.json
```

It verifies the exact seven-collection set, Mongo/manifest/Parquet row-count
agreement, published manifests, schema fingerprints, artifact byte counts,
primary-key indexes and watermarks. Full artifact rehashing remains available
through `scripts/verify_prod_baseline.py --rehash` but is intentionally not part
of every routine readback.

The safe dry plan remains:

```bash
venv/bin/python scripts/backfill_prod.py
```

To reconcile or rerun selected tables after an explicit operator decision:

```bash
venv/bin/python scripts/backfill_prod.py \
  --execute \
  --confirmation SHARADAR_PROD_WRITE \
  --production-confirmation BACKFILL_SHARADAR_PROD
```

The command re-runs the live DEV readiness gate before connecting to PROD. It
downloads each table through a credential-free second hop, requires exact
versioned headers, streams CSV with bounded memory, writes Mongo in batches,
publishes Parquet and a final manifest. Use repeated `--table` options to limit a
reconciliation run instead of unnecessarily reprocessing all seven tables.

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

## Resident PROD daily scheduler

The Mac Studio owns the resident launchd agent
`com.medina.sharadar-prod-daily`. launchd invokes the guarded runner hourly at
minute 45; the runner executes at most once per `America/New_York` service date
and only after 00:45 ET. A failed prerequisite or update leaves the service date
incomplete, so the next hourly invocation retries automatically.

Safety boundaries:

- the `/Volumes/Pentagon_Quant` SMB mount, PROD artifact root and baseline
  receipt must exist before any API or Mongo write;
- a local non-blocking file lock prevents overlapping runs;
- a successful service date is written atomically under `var/state/`;
- the API key and Mongo URI remain in Medina Vault/bootstrap files and never
  appear in the launchd plist or process arguments;
- only `tickers`, `fundamentals`, and `daily` use daily `lastupdated` deltas;
  tables without that vendor clock remain bulk-reconciliation tables.

Operational commands:

```bash
# Read-only prerequisite/schedule check
venv/bin/python scripts/run_prod_daily.py --check-only

# Operator-triggered run; still enforces NAS and overlap lock
venv/bin/python scripts/run_prod_daily.py --force

# Inspect the installed agent and logs
launchctl print gui/$(id -u)/com.medina.sharadar-prod-daily
tail -n 100 var/log/prod_daily.out.log
tail -n 100 var/log/prod_daily.err.log
```

### 2026-08-31 deployment evidence

- Installed plist:
  `/Users/chouwilliam/Library/LaunchAgents/com.medina.sharadar-prod-daily.plist`
- launchd domain and status: `gui/501`, loaded, first launch exit code `0`.
- First guarded live PROD run completed from
  `2026-08-31T03:45:58Z` to `2026-08-31T03:46:52Z`.
- Delta artifacts admitted: `tickers=13,453`, `fundamentals=1,546`,
  `daily=32,782`; all three published manifests exist.
- The immediate launchd bootstrap invocation returned `SKIPPED / already_complete`,
  confirming that the same Eastern service date was not written twice.
- Local scheduler state and logs live under the ignored `var/` directory; licensed
  artifacts remain on the PROD NAS and current rows remain in `SHARADAR_PROD`.

## Deferred tests

Large crash/concurrency/memory soaks and signed-URL-expiry reacquisition remain
deferred. The paid full-history acceptance run is no longer deferred: 7/7 tables
completed and passed the bounded PROD readback. Routine readback uses metadata,
file sizes and database collection statistics; a full 2.9GB artifact rehash is
kept as an explicit audit option rather than repeated on every run.
