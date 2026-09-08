# AI project instructions

Read `README.md` for this repository's runtime and data boundaries.

## Start here; keep context and work bounded

- Inspect the working tree, current branch and upstream before editing. Preserve
  unrelated changes; do not assume this repo uses `main` instead of `master`.
- Read only the task-relevant references below. Historical milestone counts,
  commit IDs and successful runs are evidence at that date, not live health.
  Check current state before claiming that PROD is current or a job completed.
- William prefers necessary, risk-focused verification and economical token use.
  Do not repeatedly read all docs, poll unchanged jobs, run full suites, or
  download full history to prove a small change. Delegate only when requested
  or otherwise authorized; avoid multiple agents repeating the same audit.

## Ownership and data boundaries

- This repo owns the Sharadar vendor SDK, ingestion, schema admission, raw
  captures, query-ready tables, revision lineage, manifests and watermarks.
  Factor expressions, sector neutralization, PDATA/PSTAGE/PMART, portfolio
  grouping and returns belong to downstream Pentagon Foundry / Brain Studio.
- Exact Mongo routes are `SHARADAR_DEV` and `SHARADAR_PROD`. Resolve deployment
  through `routes.py` / `runtime.py`; never build an ad-hoc database route or
  silently fall back from one environment to the other.
- NAS roots are `/mnt/nas/Medina_US_Equity/sharadar/{dev,prod}` on Linux and
  `/Volumes/Medina_US_Equity/sharadar/{dev,prod}` on macOS. Use the runtime route,
  not a hard-coded Home path in a Linux command. A directory existing is not
  sufficient proof that the NAS is mounted; retain the mount prerequisites.
- The established Fundamentals mirror contains `descriptions`, `tickers`,
  `fundamentals`, `daily`, `actions`, `events`, and `sp500`. Catalog support for
  another table does not prove entitlement or that its data was ingested.
- `daily` supplies daily valuation measurements; it is not the `stocks`/SEP
  OHLC price product. Do not substitute `metrics` for `daily`, infer complete
  high/low coverage, or mix FMP and Sharadar values without explicit downstream
  identity, date and price-basis reconciliation.
- Keep licensed rows, captures, runtime state and credentials out of Git.
  Use synthetic fixtures and aggregate counts/hashes in development notes.

## PIT and historical identity: do not blur these clocks

- `reportperiod` is the accounting period; `date` in AR fundamentals is the
  conservative filing/availability date; `calendardate` is a calendar label;
  `lastupdated` is the vendor revision/ingestion clock. They are not substitutes.
- Research-facing `SharadarReader.as_reported_fundamentals` accepts ARQ/ARY/ART
  and rejects MRQ/MRY/MRT. MR rows may exist in the vendor mirror, but must not
  silently enter an as-reported historical signal.
- The reader filters `date <= as_of`, then selects newest `reportperiod` and
  newest visible `date` within it. Date-only availability is not proof that a
  report was known before an intraday trade. Consumers must define their own
  decision/execution timing; do not silently change inclusive/exclusive rules.
- Vendor AR dates and current Mongo rows do not by themselves establish our
  historical first-observed timestamps or an immutable historical vendor view.
  Preserve capture/revision lineage and disclose the limits of a PIT claim.
- A late revision to an older quarter must not displace an already-known newer
  quarter when the requested policy is latest reporting period. Likewise,
  four *available* report periods back is not necessarily four actual quarters
  back when a quarter is missing. Test these cases when changing selectors.
- The September 7 Foundry audit recorded these downstream ART-selection risks
  in `../pentagon/foundry/docs/20260907_shared_price_repair.md`. That was not a
  defect in this reader's reportperiod-first sort. Recheck downstream status;
  do not rewrite Sharadar vendor rows to compensate for consumer formula bugs.
- Tickers can change. Preserve official table/primary-key fields and stable
  `permaticker` identity; use dated `actions` and membership evidence for joins.
  `relatedtickers` alone is not proof of a dated one-to-one identity mapping.
- Historical S&P 500 membership must retain additions, removals and re-entry;
  today's members are not a historical universe. A current master/sector label
  is not automatically the historical classification. Missing records or
  delistings must not be silently dropped, zero-filled or forward-filled.

## Ingestion, publication and recovery

- Reuse existing clients, catalog, versioned registry and storage engine.
  Preserve immutable raw bytes; schema/type/alias normalization affects only
  the admitted copy. Changed headers/types/keys require an explicit schema
  review, not a blind registry refresh or suppression of the error.
- Bulk and REST ticker table aliases must normalize consistently (for example
  SF1 versus fundamentals). `tickers.table` participates in identity; bypassing
  normalization previously produced semantic duplicate rows.
- Retain streamed downloads, bounded CSV/Parquet batches and Mongo upserts.
  Avoid per-row network calls, unbounded response reads and full-history sets.
  Do not claim O(1) memory when exact deduplication retains all keys.
- Validate bytes, checksums, schema and row/key invariants before treating a
  capture as usable. A published run manifest is the final commit marker;
  partial files or a watermark alone are not successful publication.
- Preserve deterministic identities, revision lineage, original baseline
  receipts and idempotent replay. On failure keep last-good published evidence;
  repair only the exact affected table/window/run after establishing the cause.
- Do not turn a diagnostic task into a new Bulk download, reconciliation,
  `--force` run or PROD repair. Obtain the needed explicit scope before those
  actions; retain existing route/confirmation gates rather than bypassing them.

## Production operations and credentials

- The documented resident writer is the Ubuntu supercomputer. Do not install a
  second Home scheduler or start a duplicate while daily/monthly ingestion owns
  the shared `var/run/prod_ingestion.lock`. Inspect current host/process/state
  before an authorized deployment; do not edit a running job's code in place.
- The daily runner uses America/New_York service dates and a 00:45 ET gate;
  hourly cron invocations are retry opportunities, not separate daily jobs.
  Use timezone-aware code, not a fixed Taiwan/UTC offset across DST.
- Existing policy: three-day lastupdated overlap for tickers/fundamentals/daily;
  35-day date overlap for events; actions also look 370 days into the future.
  Monthly Bulk reconciles all seven tables. Inspect implementation/current
  configuration before changing these windows or describing current scheduling.
- Actions may be future-effective: their maximum event date is not a completed
  ingestion watermark. Do not advance service state until the required stages
  succeed, fabricate success, or delete a lock to make a second runner start.
- Resolve credentials lazily via the established Vault/profile integration:
  `readonly` for inspection, `pipeline_rw` for authorized writes. Route authority
  must be checked before credential access. Never use bootstrap/root credentials
  as a shortcut, print URIs/keys, or embed them in argv, Git, manifests or logs.
- Preserve the bulk two-hop boundary: first-party authentication must not reach
  the signed download host; signed URL query strings are sensitive too.
- SSH, deployment, schedule changes and PROD data writes remain separately
  scoped actions. A passing DEV readiness check or access to a credential does
  not grant publication authority. Preserve unrelated host services and crons.

## Navigation and proportionate verification

| Task | Code and references to read | Focused test starting points |
| --- | --- | --- |
| REST/auth/pagination/Bulk | `client.py`, `auth.py`, `bulk.py`, `catalog.py`; `docs/20260831_api_sdk_test_contract.md` | `test_client.py`, `test_auth_and_pagination.py`, `test_bulk.py` |
| Routing/credentials | `routes.py`, `runtime.py`; `docs/20260905_mongodb_credential_migration.md` | `test_catalog_and_routes.py`, `test_runtime.py` |
| Schema/storage/replay | `schema_registry.py`, `storage/`, `bulk_pipeline.py`; `docs/20260831_dev_storage_and_prod_backfill_readiness.md` | `test_schema_registry.py`, `test_storage_engine.py`, `test_bulk_pipeline.py` |
| Daily/monthly/health | `scripts/run_prod_daily.py`, `run_prod_monthly.py`, `check_prod_health.py`; `docs/20260831_prod_operations_v02.md` | `test_prod_daily_scheduler.py`, `test_date_overlap_update.py`, `test_prod_monthly_scheduler.py`, `test_prod_health.py` |
| Consumer PIT queries | `reader.py`; consumer contract in the operations note | `test_reader.py` |

Code names in the table are relative to `src/sharadar_pipeline/` unless a
`scripts/` prefix is present; test files are under `tests/`.

- Use the existing compatible interpreter (project requires Python >=3.11) and
  select relevant test files, e.g. `venv/bin/python -m pytest tests/test_reader.py
  -m 'not live and not performance'`. Check actual environment availability;
  `uv.lock` runtime dependencies do not automatically include pytest from
  `requirements.txt`. Do not upgrade dependencies just to inspect a document.
- Paid `live` tests are opt-in. Streaming/performance changes need their bounded
  synthetic performance checks; routine UI/docs/isolated logic work does not
  justify network calls, a full baseline replay or a full NAS rehash.
- A documentation-only change needs content/path review and `git diff --check`,
  not ingestion or runtime tests. For data changes, distinguish planned,
  executed, read-back-verified and published states in the handoff.
- Provider documentation snapshots under `docs/sharadar_official/` are dated
  evidence. Verify current official contracts if a vendor behavior/schema change
  is relevant; do not refresh every snapshot as routine setup.
- Update the relevant repo development note after a material milestone. Record
  source/version identities, scope, targeted verification, real remaining gaps
  and the next action; never represent a synthetic test as a live PROD check.

## Standing authorization: track and push `uv.lock`

William explicitly requested this policy on 2026-09-08:

- Keep the repository-root `uv.lock` under version control. Do not ignore or
  routinely delete it as a generated temporary file.
- When repository work creates or changes `uv.lock`, review it, confirm it matches
  `pyproject.toml` (for example, `uv lock --check`), and commit and push the valid
  lockfile change to the working branch's configured upstream. This routine
  lockfile commit/push is authorized without asking William again.
- Include related dependency declarations only when they are part of the
  authorized task. Stage exact paths; do not include unrelated user changes.
- If the lockfile is unchanged, there is nothing to commit or push. Do not run
  upgrades or regenerate it solely to produce a change.
- Before pushing, check that the diff contains no credentials, unintended private
  sources, machine-specific absolute paths, or unrelated dependency changes.
  Stop and report a failed consistency check or an unexpected change rather than
  silently accepting it. Do not force-push, overwrite remote work, or invent an
  upstream when the intended destination is unclear.
- This authorization covers Git maintenance of the lockfile, not automatic
  package upgrades, runtime deployment/restarts, or Mongo/NAS data writes.
