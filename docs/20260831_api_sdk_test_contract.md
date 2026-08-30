# Sharadar API SDK test / QA contract v0.1

Date: 2026-08-31  
Status: implementation gate  
Scope: Sharadar table REST API, schema API, bulk capture, environment routing and
the reusable Python SDK boundary. Normalized Parquet and Mongo publication have
their own later data-quality contract, but their route boundary is included
here.

## 1. Outcome and testing principle

The first SDK must make every documented Sharadar table addressable through one
typed interface, make `years=5|10|full` bulk downloads safe and restartable, and
remain usable by `foundry`, notebooks and future Medina services without those
consumers knowing HTTP, Vault or signed-download details.

The test strategy is deliberately split so that correctness work does not turn
into repeated full-history downloads:

1. **Required fast suite:** synthetic unit and corner tests, no network, no
   Vault, no Mongo and no paid data. This is the commit/PR gate.
2. **Required performance suite:** deterministic local byte streams and ZIPs.
   It proves streaming, bounded memory and approximately linear work without
   consuming the subscription API.
3. **Opt-in paid live smoke:** tiny read-only requests using the Vault key. It
   proves current entitlement and vendor compatibility, but is never run by
   ordinary `pytest` and never writes `SHARADAR_PROD`.
4. **Deferred large acceptance:** one real full-history capture, normalization
   and reconciliation when the ingestion milestone is ready. It is not a unit
   test and must not be repeated on every code change.

“Complete tests” therefore means complete behavioral coverage at the SDK
boundary, not downloading hundreds of megabytes for every test run.

## 2. Official contract versus Medina policy

Facts confirmed by the 2026-08-31 official-document snapshot:

- API base: `https://api.sharadar.com/v1.0`.
- Data endpoint: `GET /data/{table}`.
- Schema endpoint: `GET /schema/{table}`; documented dialects are
  `postgres`, `sqlite` and `mysql`. The schema index is JSON.
- Data response formats: explicit `csv` or `json`; CSV is the documented
  default. The SDK always sends a format rather than relying on that default.
- Common query parameters include `ticker`, `from`, `to`, `fields`, `sort`,
  `limit`, `skip`, `format`, table fields and field comparison operators.
- Operators are equality plus `.gt`, `.gte`, `.lt` and `.lte`.
- `from`/`to` are strict `YYYY-MM-DD` dates. Most data requests default to a
  recent window if a lower bound is omitted; `tickers` has different date
  behavior. Production code must therefore be explicit.
- Bulk uses `years=5|10|full`; the data request returns HTTP 302 to a temporary
  download location. `status=True` returns bulk metadata without downloading.
- The documentation does not publish a numeric request quota or a complete
  error-code specification.

Medina engineering policy:

- Credentials are read from Vault and sent only as `x-api-key` to the
  first-party API. Although query-string authentication is supported upstream,
  this SDK does not generate credential-bearing URLs.
- Schema is captured and hashed. Table/field validation uses a versioned schema
  rather than duplicating every column in consumer code.
- Every retry policy, concurrency limit and performance threshold below is a
  client policy, not a claimed Sharadar guarantee.

## 3. Public SDK behavior to freeze with tests

Implementation names may vary, but the following separations are contractual:

```text
SharadarClient
  query(QuerySpec)                  -> bounded REST result/stream
  get_schema(table, format)         -> versioned schema artifact
  get_bulk_status(table, history)   -> metadata only
  request_bulk_redirect(table, history) -> validated ephemeral target

BulkDownloader
  capture(redirect, destination)    -> immutable capture receipt

RouteResolver
  resolve(DEV|PROD)                 -> exact DB/artifact route + write policy
```

`QuerySpec` must be typed data, not a preassembled URL. Its minimum fields are:

```text
table, format, tickers, from_date, to_date, fields, filters,
sort, limit, skip
```

Filters use `(field, operator, value)` where operator is one of
`eq/gt/gte/lt/lte`. URL encoding happens in exactly one transport component.
Callers cannot add arbitrary authentication headers or substitute a different
origin through `QuerySpec`.

The default client is read-only. Raw capture and publication are explicit
operations. A consumer importing the SDK must never create a Mongo connection,
download a bulk file or read Vault as a module-import side effect.

## 4. Complete table catalog

All current canonical table names belong in the SDK catalog even when the
current subscription does not entitle a live read. Legacy aliases are accepted
only by an explicit compatibility mapper; generated URLs use canonical names.

| Product | Canonical table | Legacy alias | Current Fundamentals live-smoke expectation |
| --- | --- | --- | --- |
| Reference | `descriptions` | `indicator`, `indicators` | entitled |
| Reference | `tickers` | none | entitled |
| Fundamentals | `fundamentals` | `SF1` | entitled |
| Fundamentals | `daily` | none | entitled |
| Fundamentals | `actions` | none | entitled |
| Fundamentals | `events` | none | entitled |
| Fundamentals | `sp500` | none | entitled |
| Prices | `stocks` | `SEP` | catalog/unit only unless entitlement changes |
| Prices | `funds` | `SFP` | catalog/unit only unless entitlement changes |
| Prices | `metrics` | none | catalog/unit only unless entitlement changes |
| Investors | `insiders` | `SF2` | catalog/unit only unless entitlement changes |
| Investors | `holdings` | `SF3` | catalog/unit only unless entitlement changes |
| Investors | `holdings_ticker` | `SF3A` | catalog/unit only unless entitlement changes |
| Investors | `holdings_investor` | `SF3B` | catalog/unit only unless entitlement changes |

Required catalog tests, parameterized over all 14 canonical tables:

- canonical table produces `/data/{canonical}` and
  `/schema/{canonical}` only;
- aliases map case-insensitively to one canonical name, but unknown tables,
  path traversal, slashes, whitespace and URL fragments are rejected locally;
- every table has product group, documentation URL and canonical-name metadata;
- no entitlement is inferred merely because a table exists in the catalog;
- primary keys are read from the captured schema. Safety-critical known keys
  such as fundamentals `(ticker, dimension, date, reportperiod)` and daily
  `(ticker, date)` receive regression assertions, but other keys are not
  guessed;
- a newly discovered upstream table or schema field causes an explicit catalog
  or schema-drift report rather than silently disappearing.

## 5. Required fast unit suite

### 5.1 Authentication, logs and redaction

Use a synthetic sentinel such as `SDK_TEST_SECRET_DO_NOT_LOG`, never a real key.
For every case below, capture stdout, stderr, structured logs, exception text,
`repr()` output, manifest JSON and fake tracing attributes and assert that the
sentinel is absent.

- key loaded lazily through the injected secret provider;
- empty, whitespace, bytes, unavailable Vault and provider exception;
- header sent as `x-api-key` only to `api.sharadar.com`;
- query-string `api_key` and `apiKey` are never generated by the SDK;
- request headers in errors, debug logs and retry logs are redacted;
- signed redirect URL is represented as origin plus `[REDACTED]`, not query;
- upstream error bodies that echo a key or URL are redacted before surfacing;
- redirects never copy `x-api-key`, cookies, authorization headers or first-party
  request metadata to the download host;
- fixtures, snapshots and golden files contain no licensed vendor rows.

Add a repository secret scan to the fast gate for `api_key=`, `apiKey=`,
`x-api-key`, common signed-URL parameters and the synthetic sentinel. Allow the
literal header name only in source/tests where needed; reject values.

### 5.2 Query construction, formats and validation

Parameterize URL construction across all tables and assert decoded query pairs,
not brittle query-string ordering.

- explicit CSV and JSON; invalid casing/values and undocumented XML are rejected;
- `ticker=AAPL` and comma-separated `AAPL,MSFT,IBM`;
- empty ticker list, empty member, duplicate member, whitespace normalization,
  punctuation in valid ticker symbols and non-string input;
- `fields` preserves requested order, removes or rejects duplicates by documented
  policy, and cannot omit required primary-key fields from ingestion requests;
- `sort=<field>.asc|desc`; no multi-sort, invalid direction or unknown field;
- positive `limit`, zero-or-positive `skip`; reject booleans, negative values,
  floats and absurd locally configured values without inventing an upstream
  maximum;
- Unicode, spaces, ampersands, equals signs, commas and percent signs are encoded
  exactly once and cannot inject a second parameter;
- duplicate semantic parameters (`from` plus a raw `date.gte`, for example) are
  resolved by typed policy or rejected rather than silently overwritten;
- calls never mutate caller-owned lists, dictionaries or filter objects;
- deterministic request representation supports stable test/debug output after
  secret redaction.

The client may provide an `allow_unknown_fields` escape hatch for newly added
upstream fields. In strict ingestion mode, table fields and sort/filter fields
must exist in the selected schema version.

### 5.3 Dates and operators

Test equality and all four documented comparison operators on string, numeric
and date fields. The encoder maps them exactly to:

```text
field=value
field.gt=value
field.gte=value
field.lt=value
field.lte=value
```

Date cases:

- leap day in a leap year; reject leap day in a non-leap year;
- month/year boundaries and `from_date == to_date`;
- reject `2023-09-31` (an invalid date appearing in an official example),
  non-zero-padded values, timestamps, timezone-bearing strings and blank values;
- reject `from_date > to_date`;
- `date` objects encode in ISO format without local-time conversion;
- no default “one year ago” is silently added by the SDK: bounded ingestion must
  supply explicit bounds; an explicitly named exploratory query may omit them;
- fundamentals `date`, `reportperiod`, `calendardate` and `lastupdated` remain
  distinct typed fields;
- the `daily` endpoint never accidentally becomes `metrics` (guarding the
  current documentation copy/paste defect).

Filter corner cases include nulls, empty multi-values, NaN/infinity, very large
integers, decimal preservation, repeated field/operator pairs and unsupported
operators (`ne`, `in`, regex). Unsupported behavior fails locally.

### 5.4 Response parsing and schema endpoints

For both CSV and JSON, test:

- empty but valid response, one row, multiple rows and the documented JSON
  envelope used by the current service;
- UTF-8 BOM, CRLF/LF, quoted commas/newlines, null/blank fields, escaped quotes,
  large integers and decimal text without premature float coercion;
- wrong content type with otherwise valid body is quarantined, not guessed in
  ingestion mode;
- HTML login/error page with status 200, truncated CSV/JSON, invalid UTF-8,
  duplicate headers and inconsistent row widths;
- HTTP status and safe response metadata survive into typed exceptions;
- parsing does not silently discard unknown fields.

For `/schema/{table}` in every catalog table:

- build PostgreSQL, SQLite and MySQL DDL schema URLs with explicit format;
- parse/inspect field name, type and primary-key metadata from
  captured/synthetic DDL fixtures; test the JSON schema index separately;
- reject empty schema, duplicate field names, duplicate/inconsistent primary-key
  entries, unknown type and malformed payload;
- canonicalize deterministically and record SHA-256/schema version;
- same schema is idempotent; changed field/type/key is reported as drift and is
  not silently promoted;
- cache is keyed by table, format/source and content hash, not only filename;
- offline tests use Medina-authored synthetic schema fixtures, not copied paid
  data.

### 5.5 HTTP status, retry, timing and cancellation

The transport accepts injected clock, sleeper and random source. Unit tests do
not actually sleep.

| Response/condition | Required result |
| --- | --- |
| 200 | parse according to explicit format |
| 204 for a data query | typed empty/unexpected-response decision; never publish unvalidated emptiness |
| 301/302/303/307/308 on ordinary REST/schema | do not automatically send credentials to another host |
| 302 on a bulk request | return an ephemeral validated redirect object |
| 400/404/other deterministic 4xx | fail without retry; safe context only |
| 401 | fail fast as authentication error; no fallback key |
| 403 | fail fast as entitlement error |
| 408/429 | retry within configured budget |
| 500/502/503/504 | retry within configured budget |
| DNS/connect reset/read timeout/truncated body | retry or fail according to operation policy |

Retry tests cover zero retries, eventual success, exhaustion, maximum-attempt
off-by-one, full jitter bounds, exponential cap, `Retry-After` seconds and HTTP
date, malformed/negative `Retry-After`, total deadline, user cancellation and
non-retriable parse/schema errors. A retry must reproduce the same logical
request and must not duplicate committed output.

Concurrency tests prove the configured semaphore is honored, one failed task
releases its permit, and defaults remain conservative. They do not assert that
Sharadar permits a particular request rate.

### 5.6 Pagination

- page shorter than `limit` terminates;
- page exactly equal to `limit` requests the next `skip`;
- final empty page terminates without adding a row;
- retrying a page does not double-yield committed rows;
- duplicate primary keys across pages are detected/deduplicated by publication
  policy and counted;
- changing payload under the same key is reported, not silently discarded;
- repeated identical full pages, server ignoring `skip`, integer overflow and
  configured max-pages prevent an infinite loop;
- partition splitting is chosen before unbounded offset growth;
- only the documented first sort field is used; client logic never assumes
  stable compound ordering.

## 6. Bulk downloader unit and corner contract

### 6.1 Status and controlled 302 handoff

- `status=True` and `years=5|10|full` are mutually clear typed operations;
- invalid years, mixed bulk/REST filters and lower-case truth-value ambiguity are
  rejected locally;
- 302 is captured with automatic redirects disabled;
- missing/multiple/relative `Location`, non-HTTPS scheme, embedded credentials,
  fragment, loopback/private target and redirect loop are rejected;
- temporary URL exists only in memory and its query never enters logs,
  exceptions, filenames or manifests;
- second request contains no Sharadar API key/header/cookie;
- redirect expiry (403/404) reacquires one fresh first-party redirect within a
  bounded policy, then fails cleanly;
- status size/name/modified timestamp are metadata, not trusted as proof of
  downloaded integrity.

### 6.2 Streaming and integrity

The downloader must iterate response chunks into a same-filesystem temporary
file while updating byte count and SHA-256. Tests fail any implementation that
uses `response.content`, unbounded `read()` or accumulates all chunks in a list.

Cases:

- 1 byte, exact chunk boundary, boundary plus one, many short reads and missing
  `Content-Length`;
- zero bytes, early EOF, received length mismatch, connection reset and timeout;
- disk full, permission error, fsync failure and process-style interruption;
- SHA-256 deterministic across different chunk sizes;
- existing immutable target with same hash is an idempotent success; same
  identity with different hash is a conflict, never an overwrite;
- only the downloader-owned partial file is removed on failure;
- file fsync and directory fsync occur before/around atomic promotion according
  to platform capability;
- capture receipt is published only after bytes and ZIP checks succeed;
- two workers for the same capture serialize through an exclusive lock or one
  loses safely; neither produces a mixed file.

HTTP range/resume is disabled unless the download host explicitly advertises
byte ranges and the implementation validates `206` plus `Content-Range`.
Without that complete contract, an interrupted file restarts from byte zero.

### 6.3 ZIP/CSV safety

- valid ZIP with exactly the expected CSV member;
- CRC failure (`ZipFile.testzip()`), truncated central directory, non-ZIP HTML,
  encrypted member and unsupported compression;
- missing CSV, duplicate expected member, unexpected nested path, case collision
  on macOS and multiple plausible CSV members;
- zip-slip paths (`../`, absolute path, drive prefix), symlink entries and null
  bytes are rejected even if extraction is not currently required;
- configurable maximum member count, uncompressed size and compression ratio
  defend against accidental zip bombs without hardcoding the current file size;
- CSV header is compared with the selected schema version before promotion;
- header reorder is either canonicalized explicitly or treated as drift; missing,
  duplicate and unexpected columns are reported separately;
- status byte count, HTTP `Content-Length`, received bytes, ZIP member size and
  final hash are recorded as distinct facts.

ZIP processing should open/stream a member directly when normalization begins;
it must not extract the whole archive into an untracked directory.

### 6.4 Atomicity, idempotence and manifest

Manifest tests use a strict allowlist. Required safe fields include table,
history, timestamps, reported/received byte counts, hash, CRC result, CSV member,
schema hash and code commit. Forbidden fields include API key, all request
headers, authenticated URL and signed redirect URL.

Test crash points before download, mid-stream, after file fsync, after rename and
before manifest commit. Recovery must yield either the old complete capture or a
new complete capture, never a “successful” half-state. Replaying the identical
capture returns the same identity/receipt; it does not redownload or mutate an
immutable object unless an explicit reconciliation policy requests it.

## 7. DEV/PROD route contract

Only these routes exist:

| Mode | Mongo control plane | Artifact root | Write policy |
| --- | --- | --- | --- |
| `DEV` | `SHARADAR_DEV` | configured DEV root | enabled |
| `PROD` | `SHARADAR_PROD` | configured PROD root | locked by default; explicit promotion authority required |

Tests must prove:

- enums/config map exact names; arbitrary suffixes, lowercase concatenation and
  user-provided database strings are rejected;
- DEV cannot resolve to a PROD database, bucket, filesystem root or watermark;
- PROD write construction fails before any network/filesystem mutation unless
  the explicit production-write policy is supplied;
- read-only PROD consumers cannot obtain a writer through type/config confusion;
- environment choice is resolved once and propagated into receipts/manifests;
- raw, normalized, schema, manifest and watermark roots all change together;
- unit tests use temporary roots and fake Mongo clients; they never touch either
  real database;
- paid live smoke is API read-only and does not write DEV or PROD;
- promotion copies/references an immutable verified build; it never recomputes
  from an untracked local file.

## 8. Required performance / complexity suite

Performance tests use generated incompressible and compressible byte streams at
multiple sizes; generated files are deleted after the test. They do not call
Sharadar.

### 8.1 Complexity budgets

| Operation | Time target | Additional memory target |
| --- | --- | --- |
| query/filter encoding with `k` values | `O(k)` | `O(k)` for the final URL only |
| streamed bulk download of `n` bytes | `O(n)` | `O(chunk_size)` |
| SHA-256 and byte count | one pass, `O(n)` | constant |
| ZIP CRC validation of `n` uncompressed bytes | `O(n)` | bounded buffer |
| CSV-to-row-group normalization | `O(n)` | bounded by configured row group/batch |
| schema/catalog lookup | average `O(1)` after load | proportional to schema, not data rows |

Exact primary-key deduplication can require state proportional to a partition.
The ingestion design must bound that state by table/date/ticker partition or use
external sort/storage; it must not claim constant memory while storing every key
from full history in a Python set.

### 8.2 Executable performance checks

Mark as `performance`, exclude from normal `pytest`, and run before an SDK
release or ingestion milestone:

1. Stream deterministic 16 MiB, 64 MiB and 256 MiB bodies through the real
   downloader and hash/file path.
2. Assert peak RSS does not grow in proportion to body size. Initial acceptance:
   256 MiB input adds at most 64 MiB over the measured baseline with the default
   chunk size; tune only with evidence.
3. Compare median time over repeated local runs. Quadrupling input should take no
   more than 6x the elapsed time after warm-up. This is a regression/linearity
   guard, not a vendor throughput promise.
4. Run valid and corrupted ZIPs at multiple sizes and prove the same bounded
   memory behavior.
5. Run CSV batches with narrow/wide rows and quoted multiline fields; peak
   memory must follow configured batch size rather than total file size.
6. Stress 10,000 filters/fields using synthetic values to catch accidental
   quadratic concatenation. The normal API still applies its configured URL
   length/field-count guard before sending.
7. Simulate two concurrent small REST streams and one bulk stream; verify limits,
   cancellation and absence of cross-request credential leakage.

Record machine, Python version, commit, fixture sizes, chunk/batch settings,
median/p95 elapsed time, throughput and peak RSS. Do not fail ordinary CI on a
single noisy wall-clock sample. Memory-bound and algorithmic-regression checks
remain release gates.

## 9. Paid live smoke contract

Live tests require both an explicit marker/flag (for example
`RUN_SHARADAR_LIVE=1`) and successful Vault lookup. Missing either produces a
skip, not a fallback to `test-api-key`.

The small, read-only suite:

1. Query one row of non-free-sample `PLTR` in `fundamentals`, `daily` and
   `tickers`; require HTTP 200, one row and requested ticker match.
2. Query one AR fundamentals slice with explicit date/dimension fields and verify
   dates parse and the primary key is complete. Do not assert a changing numeric
   value.
3. Fetch one documented DDL schema dialect for every currently entitled table
   and compare structural invariants; schema hash changes create a review
   artifact, not a blind fixture rewrite.
4. Call `fundamentals status=True` and validate safe metadata shape.
5. Request `fundamentals years=full` with redirects disabled; require 302 and a
   valid HTTPS location, then discard it without printing or downloading.

Do not probe unpurchased product tables just to assert 403 on every run. A single
opt-in entitlement-error test may use a fake transport; live entitlement changes
are configuration, not code correctness.

Live assertions never include exact row counts for moving datasets, exact signed
hosts, exact status byte size, performance latency or current financial values.
Output is limited to table, status, returned-row count, ticker match and safe
schema/status metadata.

## 10. Deferred full-history and PROD-grade tests

These are required before declaring the ingestion pipeline production-ready,
but intentionally deferred from SDK commit testing:

- one real `fundamentals years=full` download with immutable receipt, SHA-256,
  byte-count comparison, ZIP CRC and schema/header validation;
- typed Parquet conversion with row counts, primary-key null/duplicate profile,
  AR/MR dimension counts and bounded-memory evidence;
- replay the same capture and prove identical build identity/output hash;
- kill/restart at controlled checkpoints and prove atomic recovery;
- compare a bounded REST sample against rows normalized from the bulk capture;
- overlap-window incremental replay, changed-row lineage and failed-partition
  watermark rollback;
- monthly bulk-versus-incremental reconciliation;
- delisted/ticker-change/action examples and `permaticker` identity checks;
- a formal, separately authorized promotion rehearsal into `SHARADAR_PROD`;
- long-duration soak under conservative request concurrency after observing
  actual vendor behavior.

Raw paid data and generated full-history artifacts remain outside Git. Test
reports contain aggregate counts and hashes only.

## 11. Red-team checklist

Before accepting the SDK, attempt to break it in these ways:

### Secrets and origin boundaries

- force every exception path to echo request headers/body/URL;
- redirect through HTTPS to HTTP, userinfo URL, localhost, private IP, relative
  URL and multiple hops;
- place a fake key inside an upstream error body and signed query parameter;
- inspect logs, manifest, temp filenames, process arguments and pytest failure
  diffs for the sentinel;
- import the package with Vault unavailable and confirm import still succeeds.

### Query and protocol ambiguity

- inject `&api_key=...`, encoded slash, fragment, newline and duplicate
  parameters through table, ticker, field, sort and filter values;
- use invalid dates copied from documentation;
- request Daily data and verify no code path substitutes Metrics;
- send exactly 10,000 rows forever or ignore `skip` to expose infinite loops;
- return success status with HTML, schema JSON with data shape, and CSV with
  duplicate columns.

### Bulk and filesystem

- interrupt every chunk boundary and atomic-publish checkpoint;
- report one content length and send another; change chunk sizes between retries;
- corrupt only the final ZIP bytes/CRC;
- construct zip-slip, symlink, case-collision, duplicate-member and zip-bomb
  archives;
- pre-create target, lock and temp files with conflicting hashes;
- run two writers and verify immutable capture/manifest cannot diverge;
- fill disk after data write but before manifest write;
- ensure cleanup cannot delete a user directory or an earlier valid capture.

### Data and environment correctness

- return duplicate primary keys with identical and different payloads;
- change an upstream schema type or primary key;
- mix AR and MR dimensions and verify PIT consumers cannot silently accept MR;
- confuse `reportperiod`, `date`, `calendardate` and `lastupdated`;
- force DEV config to reference one PROD component and require immediate failure;
- attempt a PROD writer without promotion authority and verify zero side effects.

### Resource exhaustion

- tiny chunks, slow reads, very wide CSV rows, one enormous quoted field,
  excessive ZIP members and highly compressible content;
- unbounded retry headers, huge response headers and enormous query lists;
- cancellation while holding HTTP, file and concurrency resources;
- verify descriptors, temp files, locks and semaphore permits are released.

## 12. Definition of done

The SDK/API milestone is complete when:

- every table and schema endpoint is accessible through the typed catalog;
- fast unit/corner suite passes offline and contains no paid data;
- synthetic performance suite demonstrates linear streaming and bounded memory;
- one opt-in paid live smoke passes for current Fundamentals entitlements;
- bulk status and controlled 302 handoff are proven without a full download;
- secrets and signed URLs survive the red-team sentinel scan;
- DEV/PROD route isolation tests pass and PROD remains locked;
- the known limitations and deferred full-history acceptance items remain visible
  rather than being represented as completed.

## 13. Source references

- [Official documentation index](sharadar_official/00_SOURCE_INDEX.md)
- [Fundamentals and table semantics](sharadar_official/10_FUNDAMENTALS_AND_TABLES.md)
- [API and ingestion runbook](sharadar_official/20_API_AND_INGESTION_RUNBOOK.md)
- [Sharadar query documentation](https://sharadar.com/docs/getting-started)
- [Sharadar bulk documentation](https://sharadar.com/docs/bulk)
- [Sharadar authentication](https://sharadar.com/docs/auth)
