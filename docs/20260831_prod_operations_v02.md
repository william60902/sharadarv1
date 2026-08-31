# SHARADAR_PROD Operations v0.2

Date: 2026-08-31  
Status: deployed on `medina-supercomputer` (Ubuntu 22.04)

## Outcome

The Sharadar vendor mirror now has a closed unattended operating loop:

```text
Sharadar API
  ├─ daily lastupdated overlap ── tickers / fundamentals / daily
  ├─ daily date overlap ───────── actions / events
  └─ monthly full Bulk ────────── all seven entitled tables
                    ↓
          immutable raw + Parquet lineage
                    ↓
             SHARADAR_PROD current
                    ↓
          health report + PIT-safe reader
```

| Job | Operational label | Policy |
| --- | --- | --- |
| Daily update | `com.medina.sharadar-prod-daily` | Once after 00:45 ET; hourly retry until success |
| Monthly reconciliation | `com.medina.sharadar-prod-monthly` | Once per month after day 2, 03:15 ET; missed-run catch-up |
| Health | `com.medina.sharadar-prod-health` | Every two hours; notify only on failure/recovery transition |

Daily and monthly writers share one non-blocking PROD ingestion lock. Both
refuse to proceed when `/mnt/nas/Medina_US_Equity` or the PROD artifact root is
unavailable. Secrets remain in Medina Vault/bootstrap and do not enter plist,
argv, manifests, or logs.

## Update policy by table

| Table | Daily | Reconciliation | Reason |
| --- | --- | --- | --- |
| `tickers` | 3-day `lastupdated` overlap | Monthly full Bulk | Snapshot changes require capture lineage |
| `fundamentals` | 3-day `lastupdated` overlap | Monthly full Bulk | Detect new filings and revisions |
| `daily` | 3-day `lastupdated` overlap | Monthly full Bulk | Daily valuation measurements |
| `actions` | 35-day history plus 370-day future-effective window | Monthly full Bulk | No `lastupdated`; future splits/actions exist |
| `events` | 35-day date overlap | Monthly full Bulk | No `lastupdated`; material-event index |
| `sp500` | — | Monthly full Bulk | Small historical membership table |
| `descriptions` | — | Monthly full Bulk | Small schema/indicator snapshot |

The actions checkpoint is the completed service date, not `max(date)`: the
vendor contains future-effective corporate actions, so `max(date)` is not a
valid ingestion clock. Raw captures remain immutable; current normalized rows
are primary-key upserts.

## Ticker identity admission correction

The first live daily run exposed a real Bulk/REST representation mismatch:

- Bulk `tickers.table`: `SF1`, `SEP`, `SFP`, `SF2`, `SF3B`, ...
- REST `tickers.table`: `fundamentals`, `stocks`, `funds`, `insiders`,
  `holdings_investor`, ...

Because `table` is part of the official ticker primary key, the unnormalized
spellings initially created 13,453 semantic duplicates. The admission layer
now canonicalizes only the normalized copy while preserving raw wire values.
The affected delta was removed from Mongo using its exact immutable run ID,
the baseline aliases were migrated, and the same REST window was replayed under
`incremental-v2`.

Final evidence:

- `tickers` current rows: `74,078`;
- legacy alias rows: `0`;
- replayed REST rows: `13,453`;
- no collection-count inflation after replay.

## Live acceptance

Date-overlap PROD smoke completed successfully:

- `actions`: 5,735 rows, window `2026-07-26` through `2027-09-04`;
- `events`: 13,024 rows, window `2026-07-26` through `2026-08-30`.

The moving-watermark baseline verifier was corrected to retain the original
full-history run IDs while separately validating current watermark manifests.
After all deltas and the ticker repair it reports `PASS`: the original
`46,708,785`-row baseline remains contained in the current `46,708,899` rows
(114 newly admitted fundamental keys).

The live reader returned two rows each for AAPL/MSFT:

- latest `ARQ` known by 2026-08-30, including revenue, gross profit, net income,
  and assets;
- latest daily metrics on or before 2026-08-30, resolved to 2026-08-28.

The health report is `PASS` for both scheduler service states, all seven current
watermark manifests, raw/Parquet receipts, Mongo collection set, and exact
unique primary-key indexes. Reports are written to:

- local: `var/health/latest.json`;
- NAS: `/mnt/nas/Medina_US_Equity/sharadar/prod/health/latest.json`.

The production cutover was accepted directly on `medina-supercomputer`:

- remote daily state: service date `2026-08-31`, with both `lastupdated` and
  `date_overlap` complete;
- remote monthly state: service month `2026-08`, initialized from the verified
  full-history baseline;
- baseline readback: `PASS`, seven exact collections and `46,708,899` rows;
- crontab marker `MEDINA SHARADAR_PROD`: installed exactly once while preserving
  all unrelated host jobs;
- repository/runtime: commit `85b5986`, Python `3.12.13`, isolated `venv`.

One acceptance-only health defect was found and corrected: a forced daily run
may legitimately complete a service date newer than the current Eastern-time
gate. Freshness therefore means `completed >= expected`, not strict equality.
Malformed or older state still fails the check. The same monotonic rule applies
to monthly reconciliation.

The first Mac launchd deployment was not retained: an unattended Python process
could see the SMB mount but blocked when opening network-volume files under the
macOS TCC context. The always-on Ubuntu supercomputer has a verified persistent
CIFS mount and is the sole scheduler/writer. Mac launchd entries were removed
after the remote cutover to prevent duplicate PROD writers.

The Ubuntu host originally exposed only Python 3.10, while this repo requires
Python 3.11 or newer. Python 3.12.13 and its venv package were installed from the
Deadsnakes Jammy PPA for the isolated Sharadar environment. Package installation
also surfaced an already-pending `nvidia-dkms-535` configure failure; Python and
venv completed successfully, and the unrelated GPU driver was deliberately not
modified as part of this deployment.

## Consumer contract

`SharadarReader` is read-only and bounded. Its research-facing methods are:

- `as_reported_fundamentals(tickers, as_of, dimension, fields)`;
- `daily_metrics_as_of(tickers, as_of, fields)`;
- `table_rows(table, query, fields, sort, limit)` for bounded vendor-table reads.

The formal fundamentals method accepts only `ARQ`, `ARY`, or `ART`. It rejects
MRQ/MRY/MRT so a consumer cannot silently inject present-day restatements into
a historical PIT factor. This interface supplies vendor measurements; PDATA,
PSTAGE, and PMART remain downstream responsibilities.

## Deferred PROD-grade tests

The following remain intentionally deferred rather than blocking v0.2:

- multi-hour network partition and NAS remount soak;
- full monthly Bulk execution under cron (the same seven-table command
  already completed during the initial baseline);
- external paging integrations beyond the local macOS transition notification;
- disaster recovery on a second machine from immutable raw artifacts.
