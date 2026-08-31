# sharadarv1

Sharadar point-in-time US fundamentals ingestion pipeline with immutable bulk
captures, schema-admitted Parquet datasets, revision lineage, and reproducible daily
updates.

## Runtime boundary

- Mongo: `SHARADAR_DEV` and `SHARADAR_PROD`.
- NAS: `/mnt/nas/Medina_US_Equity/sharadar/{dev,prod}` on the Ubuntu
  supercomputer; `/Volumes/Medina_US_Equity/sharadar/{dev,prod}` on macOS.
- Vendor layer only: raw captures, query-ready vendor tables, Parquet, schemas,
  manifests, and watermarks.
- Factor artifacts remain in the separate `PDATA -> PSTAGE -> PMART` system.

## Operational commands

```bash
# Paid endpoint canary without writing data
venv/bin/python scripts/check_api_access.py

# Bounded seven-table DEV materialization
venv/bin/python scripts/ingest_dev_canary.py \
  --confirmation SHARADAR_DEV_WRITE

# Read-only DEV promotion gate
venv/bin/python scripts/verify_dev_readiness.py \
  --output /Volumes/Medina_US_Equity/sharadar/dev/readiness/latest.json

# Revalidate DEV and print the PROD full-history plan; does not write PROD
venv/bin/python scripts/backfill_prod.py

# Read-only verification of the published PROD baseline
venv/bin/python scripts/verify_prod_baseline.py \
  --output /Volumes/Medina_US_Equity/sharadar/prod/readiness/latest.json

# Inspect the resident daily scheduler prerequisites without writing
venv/bin/python scripts/run_prod_daily.py --check-only

# Read-only PROD operations health report
venv/bin/python scripts/check_prod_health.py --compact
```

The initial PROD baseline completed on 2026-08-31. Reconciliation and selected
table reruns remain protected by `--execute` and two exact confirmations; see
`docs/20260831_dev_storage_and_prod_backfill_readiness.md`.

Three cron entries on `medina-supercomputer` now operate PROD:

- `com.medina.sharadar-prod-daily`: once after 00:45 America/New_York, updating
  `tickers`, `fundamentals`, and `daily` by `lastupdated`, plus bounded date
  overlap for `actions` and `events`;
- `com.medina.sharadar-prod-monthly`: full seven-table Bulk reconciliation once
  per Eastern month after day 2 at 03:15 ET;
- `com.medina.sharadar-prod-health`: bounded health check every two hours, with a
  system-journal alert on failure/recovery transitions.

## Consumer example

```python
from datetime import date
from sharadar_pipeline import SharadarReader

with SharadarReader.connect("prod") as reader:
    fundamentals = reader.as_reported_fundamentals(
        ["AAPL", "MSFT"],
        date(2026, 8, 30),
        dimension="ARQ",
        fields=("revenue", "gp", "netinc", "assets"),
    )
    valuation = reader.daily_metrics_as_of(
        ["AAPL", "MSFT"],
        date(2026, 8, 30),
        fields=("marketcap", "pe", "pb", "ev"),
    )
```

The PIT reader rejects MRQ/MRY/MRT by design. Raw vendor access remains
available through bounded `table_rows`; factor calculations still belong in
the separate `PDATA -> PSTAGE -> PMART` system.
