# SHARADAR_DEV → PROD readiness gate

Date: 2026-08-31

## Purpose and boundary

`scripts/verify_dev_readiness.py` is the last read-only gate after the seven-table
Fundamentals canary has been written to `SHARADAR_DEV`.  It reads Mongo current
collections and the committed NAS manifests under:

`/Volumes/Medina_US_Equity/sharadar/dev`

It cannot select PROD, does not write either database, and always emits
`prod_write_authorized: false`.  A green report means the full-history PROD
backfill is prepared for an explicit operator decision; it does not start it.

## Required evidence

For each of `descriptions`, `tickers`, `fundamentals`, `daily`, `actions`,
`events`, and `sp500`, the gate requires:

- a committed manifest-last publication and a non-empty run;
- Mongo run rows exactly matching the Parquet manifest row count;
- no null or duplicate primary keys;
- the manifest schema fingerprint matching the committed schema registry;
- required PIT clock fields and their safe ordering where applicable;
- fresh SHA-256 and byte-count verification for raw and Parquet artifacts;
- deterministic replay identity (run ID and content-addressed artifact paths);
- the unique primary-key index plus run/source operational indexes; and
- a committed source watermark pointing to the same run manifest.

The fundamentals collection must additionally contain at least one ARQ issuer
with two distinct report periods.  The DEV fetcher currently asks for two ARQ
quarters for every configured canary ticker, so this promotion check is a lower,
fail-closed storage/readback floor rather than the fetch-time business check.

PIT clock validation here is structural.  It does not claim that historical
data has been independently proven strict PIT, nor does it replace the retained
Sharadar ARQ/ARY/ART lineage and later research validation.

## Supercomputer command

From the repository virtual environment:

```bash
cd /Users/chouwilliam/Medina/sharadarv1
venv/bin/python scripts/verify_dev_readiness.py \
  --output /Volumes/Medina_US_Equity/sharadar/dev/readiness/latest.json
```

Exit codes:

- `0`: all gates passed and PROD backfill may be proposed;
- `1`: verification completed but one or more checks failed;
- `2`: verification could not complete safely.

The JSON is suitable for scheduling and runbooks.  Human-readable logs should
refer to failed `checks` without copying credentials, signed URLs, or vendor
payloads.

## PROD promotion lock

The future backfill command must still pass both independent confirmations:

1. route write confirmation: `SHARADAR_PROD_WRITE`;
2. full-history promotion confirmation: `BACKFILL_SHARADAR_PROD`.

Neither token is accepted by the readiness CLI.  Keep the initial PROD operation
manual, bulk-first, resumable, and manifest-last.  Daily REST `lastupdated`
increments and periodic bulk reconciliation begin only after the initial PROD
readback has passed the same data-quality checks.
