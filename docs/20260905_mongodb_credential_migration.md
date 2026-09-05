# MongoDB credential migration

Sharadar route authorization still occurs before any credential access.
Authorized read-only inspection resolves the canonical `readonly` profile;
confirmed ingestion and reconciliation writes resolve `pipeline_rw`. Direct
use of the Vault bootstrap Mongo credential has been removed.

The existing DEV and PROD confirmation gates remain unchanged. Unit tests use
an injected connector and do not contact Vault or MongoDB.
