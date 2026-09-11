# Portable evidence and recurring PROD admission

## Cause and boundaries

Seven DEV readiness failures were the same path portability defect, not seven
corrupt tables. Historical metadata declared the former Mac NAS mount. Linux
rejected those absolute paths before table checks. All 14 raw/Parquet receipts
were independently found under the current root with matching sizes and SHA-256.

The monthly runner also reused initial promotion's live DEV gate. Monthly
maintenance now explicitly selects recurring PROD admission, while ordinary
backfill retains its original DEV readiness gate. No skip-validation flag exists.

## Implementation

artifact_paths.py allows only current-root paths, relative keys and exact known
Mac/Linux mount prefixes for the requested deployment. After relocation, resolved
paths must remain inside the root. Traversal, symlink escape, arbitrary roots and
cross-deployment prefixes fail. DEV evidence and PROD baseline receipts use it;
historical manifests, raw bytes, hashes and run identities are not rewritten.
New metadata format migration to relative keys remains separate, not required
to recover these immutable historical receipts.

Monthly adds --recurring-prod to the existing child command. It runs read-only
verify_prod_baseline.py --require-pinned-baseline before opening a write runtime.
The pinned baseline must be the recognized PROD PASS report with exactly all
seven approved table receipts and valid run identities. Missing, invalid or
partial baseline cannot fall back to current watermarks. Current collection set,
schema fingerprints, unique keys, counts, artifact sizes and publication markers
are checked against the pinned original baseline. Baseline file SHA is checked
for movement during verification and included in plan evidence.

This bounded metadata check is not a full current-row or artifact rehash audit.
Ingestion retains existing schema admission, checksums and manifest publication.
Both write confirmations, the shared monthly/daily lock, and success-only monthly
state update remain unchanged. No clock, source data, baseline or cron was changed.

## Verification and deployment status

Focused path and recurring-gate tests include positive legacy paths, traversal,
symlinks, wrong deployment, missing/failed/partial/duplicate baseline, routing
and failure refusal. Existing readiness and monthly scheduler tests also pass.
Initial isolated test collection lacked the sibling pm package; setting the
existing Medina PYTHONPATH fixed the environment without dependency changes.

Real read-only verification from Home against the mounted NAS and established
runtime credentials now returns DEV ready with zero failures and PROD baseline
PASS for all seven tables (46,775,762 metadata-count rows at inspection).
Baseline SHA: ff4a4b7e7c3a8f376d2fc4fd7b88cf2a5ae6e4a47355a59126aacdebabfc0179.
No provider download, PROD data write, or monthly execution occurred.

Deploying into the resident checkout can unblock the existing hourly :15 retry
and thereby initiate full seven-table reconciliation. Obtain that explicit scope
before deployment; do not treat a code-only deployment as inert. Inspect active
writers and the shared lock, preserve baseline/rollback evidence, then verify
all seven published results and monthly success before reporting recovery.
