# Sharadar API and Ingestion Runbook

Status: design baseline (official documentation review)
Reviewed: 2026-08-31
Scope: authentication, REST slices, pagination, bulk bootstrap, daily incremental ingestion, and failure handling

## Decision summary

Use two ingestion paths:

1. **Bootstrap and reconciliation:** download the subscribed history as Sharadar's prepared bulk zip (`years=full`). Store the received zip immutably with a manifest and checksum before normalizing it.
2. **Daily updates:** query each subscribed table with `lastupdated.gte=<overlap date>`, partition requests into bounded ticker cohorts, deduplicate/upsert by the table's documented primary key, and advance the watermark only after the complete run is durable.

Do not page through an entire table to reproduce a bulk download. Sharadar explicitly describes bulk as the easiest method for full tables, while REST is for limited, filtered retrieval.

The current Fundamentals subscription exposes these tables according to Sharadar's official AI reference: `descriptions`, `tickers`, `fundamentals`, `daily`, `actions`, `events`, and `sp500`. This runbook does not assume access to `stocks`, `funds`, or `metrics`.

## 1. API identity and authentication

Base URL:

```text
https://api.sharadar.com/v1.0
```

Sharadar's official documentation requires an API key for data queries. The official AI reference says the direct API accepts any of:

- query parameter `api_key`
- query parameter `apiKey`
- request header `x-api-key`

Implementation policy:

- Read the secret at runtime from Medina Vault into memory.
- Prefer `x-api-key` for the first-party API request so the key does not enter ordinary URLs, shell history, proxy logs, or exception messages.
- Never hardcode the key, put it in a notebook, include it in a manifest, or serialize request headers.
- Redact `api_key`, `apiKey`, `x-api-key`, and signed redirect query strings from application logs.
- Do not print the full bulk redirect URL; it is time-limited and credential-like.

The documented `test-api-key` is only for the free sample universe. It must not be used as a fallback for a paid ingestion run. Sharadar states that HTTP 403 with `Exceeds free tier` means the request is operating under free-tier access rather than the paid key.

## 2. REST query contract

Data endpoint:

```text
GET /data/{table}
```

Common documented parameters:

| Parameter | Meaning | Design use |
| --- | --- | --- |
| `format` | `csv` (default) or `json` | Prefer CSV for bulk-like slices and JSON only where structured error inspection is useful. |
| `ticker` | One or comma-separated tickers | Partition incremental requests into bounded cohorts. |
| `from`, `to` | Date bounds, `YYYY-MM-DD` | Explicitly set both for date-bounded pulls; do not rely on defaults. |
| `fields` | Comma-separated returned columns | Always include the full primary key, `lastupdated`, and all requested payload columns. |
| `sort` | One field with `.asc` or `.desc` | Sharadar documents only one effective sort field. Do not assume compound stable ordering. |
| `limit` | Row count; default 10,000 | Treat 10,000 as the documented default, not as proof of a universal maximum. |
| `skip` / `offset` | Rows skipped | Use only inside an already bounded partition. |
| `lastupdated` | Record update date; supports operators | Incremental change filter, normally `lastupdated.gte=...`. |
| `years` | `5`, `10`, or `full` | Switches to bulk-download behavior. |
| `status=True` | Bulk file metadata/status | Preflight and reconciliation metadata; does not download the payload. |

Documented comparison operators are `=`, `.gt=`, `.gte=`, `.lt=`, and `.lte=`. Date values are calendar dates in `YYYY-MM-DD` format.

Sharadar notes that omitting a lower date bound generally defaults a query to one year of data. The `tickers` table is an exception: its documentation says it has no default date window, and `from`/`to` constrain `lastpricedate`. Therefore every production request should make its intended bounds explicit instead of relying on endpoint defaults.

### Fundamentals-specific fields

The `fundamentals` endpoint additionally supports `calendardate` and `dimension`. For point-in-time research, retrieve the As-Reported dimensions (`ARQ`, `ARY`, `ART`); Most-Recent dimensions (`MRQ`, `MRY`, `MRT`) are restatement-aware and are not a substitute for the as-known history.

The official table metadata marks the fundamentals primary key as:

```text
(ticker, dimension, date, reportperiod)
```

The daily table primary key is:

```text
(ticker, date)
```

Do not infer storage keys from these two examples for other tables. Fetch or version the public table schema and use each table's documented primary-key columns.

### Safe pagination pattern

Sharadar's REST pagination is offset-based. Because only one sort field is documented and concurrent table updates can shift offsets, offset pagination alone is not a robust way to mirror a large changing table.

Use this order of preference:

1. Use the bulk zip for a full table.
2. For daily updates, partition first by a deterministic ticker cohort and a small `lastupdated` overlap window.
3. Request `limit=10000` inside each bounded partition.
4. If a partition returns exactly the limit, continue with `skip=10000`, `skip=20000`, and so on, while retaining the same filter and sort.
5. Deduplicate all pages by documented primary key before publishing.
6. If one bounded partition repeatedly reaches the limit, split it into smaller ticker cohorts or date windows rather than increasing offsets indefinitely.

A row count equal to `limit` is only a signal to fetch another page; a shorter page completes that partition. Retrying any page must be idempotent.

## 3. Bulk bootstrap

Sharadar provides prepared compressed CSV files through the normal table endpoint:

```text
GET /data/fundamentals?years=full
```

The response is HTTP 302 to a time-limited download URL. The official table documentation also exposes:

```text
GET /data/fundamentals?status=True
```

which returns file metadata such as name, size, and modified time without downloading the zip.

### Required bootstrap sequence

1. Call `status=True`; record table, subscribed history, reported byte size, and remote modified timestamp in a run manifest.
2. Request `years=full` with redirects disabled.
3. Require a redirect response and extract `Location` in memory.
4. Download the time-limited URL in a second request **without forwarding the Sharadar API-key header**.
5. Stream to a temporary file; never buffer a multi-hundred-megabyte zip in RAM.
6. Record received byte count and SHA-256.
7. Validate that the file is a readable zip, run the zip CRC check, locate the expected CSV, and validate its header against the versioned schema.
8. Atomically promote the zip and manifest to immutable raw storage.
9. Normalize from the immutable raw object into typed Parquet. Do not overwrite the raw capture.

The two-request redirect sequence prevents a custom authentication header from accidentally being forwarded to the separate download host. It also lets the downloader apply different timeout and streaming policies to the large payload.

### Bulk manifest minimum

```yaml
source: sharadar
api_base: https://api.sharadar.com/v1.0
table: fundamentals
history: full
requested_at_utc: ...
status_modified_at_utc: ...
status_reported_bytes: ...
received_bytes: ...
sha256: ...
zip_crc_ok: true
csv_member: ...
schema_version: ...
ingestion_code_commit: ...
```

Never store the API key, request header, first-party authenticated URL, or time-limited redirect URL in this manifest.

`tickers` and `descriptions` are snapshot tables: Sharadar states that `years=5`, `years=10`, and `years=full` return the same full snapshot. Other history tables may expose distinct history-length files.

## 4. Daily incremental run

Sharadar documents `lastupdated` as the date a record was last updated and explicitly shows `lastupdated.gte=...` for retrieving recently updated records. Its precision is a date, not an event sequence number, so the integration must use an overlap.

Recommended job:

```text
load durable watermark
  -> overlap_start = watermark_date - 3 calendar days
  -> refresh tickers changed since overlap_start
  -> query each enabled table with lastupdated.gte=overlap_start
  -> partition by ticker cohorts where applicable
  -> capture response + run manifest
  -> validate schema and primary keys
  -> deduplicate/upsert by primary key
  -> publish normalized Parquet
  -> advance watermark only after every partition succeeds
```

The three-day overlap is an engineering safety margin, not a vendor guarantee. Make it configurable. It protects date-boundary runs, late ingestion, and retries; primary-key upsert makes the repeated rows harmless.

### Suggested schedules

The fundamentals documentation currently states deliveries at 17:30 and 23:30 US Eastern, while the daily table states 19:00 US Eastern, both with reporting lag under one day. Schedule the Medina job after the later delivery plus a buffer, expressed in `America/New_York` rather than a fixed UTC offset so daylight-saving transitions are correct.

A practical first version is one daily run after 00:30 US Eastern, plus an operator-triggered retry. Do not encode the published delivery times as a completeness guarantee; record the observed maximum `lastupdated` and row counts per run.

### Incremental correctness rules

- The watermark represents the latest **successfully committed ingestion window**, not the largest source value seen in a partial response.
- Never advance it when one table or one partition fails.
- Re-running a completed window must produce the same normalized keys and safely replace identical rows.
- A changed payload for an existing primary key should be retained in raw lineage and replace the current normalized value; report the changed-key count.
- Empty updates are successful only after the request, entitlement, schema, and response format have been validated.
- Store table-level counts: received, parsed, unique keys, inserted, changed, unchanged, rejected.
- Run a periodic bulk reconciliation (initially monthly) to detect missed rows, historical corrections, or client-side ingestion bugs.

### Important documentation defects to guard against

Two examples in the current official pages should not be copied literally:

- Some parameter examples use `2023-09-31`, which is not a valid calendar date.
- The Daily Fundamentals page's “updated since” example points to `/data/metrics` rather than `/data/daily`.

Generate URLs from typed parameters and validate dates/table names locally instead of assembling examples by string copy/paste.

## 5. Failures, retry, and throttling

Sharadar's public documentation does **not** currently publish a numeric requests-per-minute quota, a concurrency limit, or a complete HTTP error-code contract. Do not invent one or claim a particular rate is permitted.

Known from the official material:

- HTTP 302 is expected for a bulk-download request and leads to a time-limited URL.
- HTTP 401 indicates missing/invalid access according to the official AI reference.
- HTTP 403 with `Exceeds free tier` means the request is operating with free-tier access rather than the expected paid key.

Client policy:

| Condition | Action |
| --- | --- |
| 302 on `years=...` | Follow using the controlled two-request download flow. |
| 401 | Fail fast; do not retry with `test-api-key`; report Vault/authentication configuration. |
| 403 / entitlement error | Fail fast; report table/history entitlement and account state. |
| 404 or other deterministic 4xx | Fail the partition and surface the response after redaction; inspect endpoint/parameters. |
| 408, 429, transient 5xx, connection reset, timeout | Retry with capped exponential backoff and full jitter; honor `Retry-After` if supplied. |
| Malformed CSV/JSON, HTML login page, schema drift | Quarantine the response, do not publish, do not advance watermark. |
| Checksum/zip CRC/byte-count failure | Delete only the temporary partial file and redownload; preserve failure metadata. |

Handling 429 is defensive client behavior, not evidence that Sharadar publicly documents a particular 429 policy.

Begin with conservative bounded concurrency (for example, two in-flight API requests and a configurable inter-request delay). Observe latency and error headers before tuning. Bulk files should be streamed one at a time. The absence of a published quota is not permission for unbounded parallelism.

Recommended retry defaults for transient small REST requests:

```text
attempts: 5
base delay: 1 second
cap: 60 seconds
jitter: full
connect timeout: 10 seconds
read timeout: 60 seconds
```

Large bulk downloads need a longer read timeout and resumability only if the download host advertises byte-range support; do not assume resumability without checking response headers.

## 6. Minimal acceptance checks

This project prioritizes implementation over exhaustive testing. The first ingestion release needs only these high-value checks:

1. No secret or signed download URL appears in logs, manifests, test fixtures, or Git-tracked files.
2. Bulk: HTTP redirect is controlled, bytes are nonzero, ZIP CRC passes, expected CSV exists, header matches schema, and SHA-256 is recorded.
3. Incremental: primary key is non-null/unique after deduplication and `lastupdated` parses as a real date.
4. PIT lane: every formal fundamental row uses an `ARQ`, `ARY`, or `ART` dimension unless the output is explicitly labeled non-PIT.
5. Idempotence: replaying one small overlap window leaves normalized row count and values unchanged.
6. Watermark: a simulated failed partition does not advance it.
7. Reconciliation: a small sampled REST slice agrees with the corresponding normalized bulk rows after key/type normalization.

Everything else can be recorded in a separate deferred PROD test plan.

## 7. Implementation order

1. Vault-backed secret adapter and redacting HTTP client.
2. Public schema fetch/versioning (`GET /schema/{table}?format=...`).
3. `status=True` preflight and controlled bulk downloader.
4. Immutable raw manifest plus ZIP/CSV validation.
5. Bulk normalizer to Parquet.
6. `lastupdated` incremental reader with ticker partitioning and idempotent upsert.
7. Durable watermark and run statistics.
8. Daily scheduler and monthly bulk reconciliation.

## Official sources

- [Introduction](https://sharadar.com/docs/intro)
- [Authentication](https://sharadar.com/docs/auth)
- [Querying the data](https://sharadar.com/docs/getting-started)
- [Bulk downloads](https://sharadar.com/docs/bulk)
- [Frequently asked questions](https://sharadar.com/docs/faqs)
- [Fundamentals table](https://sharadar.com/docs/fundamentals)
- [Daily fundamentals table](https://sharadar.com/docs/daily)
- [Tickers and metadata table](https://sharadar.com/docs/tickers)
- [Official AI reference (`llms.txt`)](https://sharadar.com/llms.txt)
