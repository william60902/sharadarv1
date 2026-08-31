# sharadarv1

Sharadar point-in-time US fundamentals ingestion pipeline with immutable bulk
captures, schema-admitted Parquet datasets, revision lineage, and reproducible daily
updates.

## Runtime boundary

- Mongo: `SHARADAR_DEV` and `SHARADAR_PROD`.
- NAS: `/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/{dev,prod}`.
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
  --output /Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/dev/readiness/latest.json

# Revalidate DEV and print the PROD full-history plan; does not write PROD
venv/bin/python scripts/backfill_prod.py

# Read-only verification of the published PROD baseline
venv/bin/python scripts/verify_prod_baseline.py \
  --output /Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/prod/readiness/latest.json

# Inspect the resident daily scheduler prerequisites without writing
venv/bin/python scripts/run_prod_daily.py --check-only
```

The initial PROD baseline completed on 2026-08-31. Reconciliation and selected
table reruns remain protected by `--execute` and two exact confirmations; see
`docs/20260831_dev_storage_and_prod_backfill_readiness.md`.

`com.medina.sharadar-prod-daily` is installed as a Mac Studio launchd agent. It
checks hourly at minute 45, runs once after 00:45 America/New_York, retries after
failures, and updates only the three tables with a documented `lastupdated`
watermark: `tickers`, `fundamentals`, and `daily`.
