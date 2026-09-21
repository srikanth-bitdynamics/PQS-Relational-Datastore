# Relational projection operations

The relational backend exposes SQL views. An HTTP Query API and document-to-relational migration tooling are not available.

For initial setup and query examples, see the [relational datastore guide](README.md).

## Compatible upgrades

Stop ingestion, then apply the projection. Apply adds typed columns and requires that no ingest writer is live, so that its schema changes cannot contend with in-flight batches. Backfill is resumable and may run with ingestion restarted. Activation also requires that no ingest writer is live, and requires backfill through the current published watermark. If ingestion advanced after backfill, catch up before activation. Start ingestion again after activation.

Writers and backfills bind the saved projection to the current package dictionary. Appended enum constructors are decoded using the extended case list. Reordered or removed constructors, renamed/repositioned fields, incompatible nullability, and incompatible Daml primitive types are rejected for an active promoted field. Unknown enum ordinals fail explicitly; they are never substituted with SQL NULL. Optional fields absent from older payloads remain nullable.

New projection definitions persist Daml type metadata as well as SQL types. Existing definitions without that metadata remain readable; compatibility is checked against the available package dictionary and stored enum cases.

Backfill holds the projection session lock across chunk commits. Concurrent apply and activate operations wait, and a second backfill fails with a retryable operator message. A failed backfill preserves previously committed chunks and releases its session lock. Retry the same draft after resolving the failure.

## Public view compatibility

Republishing uses `CREATE OR REPLACE VIEW`, retaining the view identity, owner, table and column grants, comments, and dependent views. Existing column positions remain stable. New columns are appended, including after `payload_json` when that column already exists. Consumers should name their columns explicitly.

Removing a published field or template is intentionally rejected. Leaving an orphaned view would expose typed values that the new writer no longer maintains. To narrow a query workload without changing the public contract, retain the published fields in the projection and reduce its configured queries/indexes.

A breaking public change requires a consumer migration during a maintenance window: identify dependent views and grants, migrate the consumers, explicitly remove the obsolete view using `DROP VIEW ... RESTRICT`, and activate the new projection. Reapply the intended grants to any newly created view. Activation never uses `CASCADE` and never drops consumers automatically. There is currently no dedicated CLI command for this breaking migration.

## Writer recovery

Use `--pipeline-ledger-start=Oldest` for resumable ingestion. On an empty datastore it starts at the ledger's available beginning; on retries it resumes at the published checkpoint. Explicit `Genesis` remains a fixed start request and the existing pipeline validator rejects it once it precedes the datastore's first checkpoint. Changing rights on retry creates a new coverage segment; it does not backfill earlier transactions for newly visible parties.

The writer holds a dedicated liveness connection. Each data transaction verifies that connection's PID and backend start time and holds a shared activity lock through commit. A successor writer, schema apply, projection apply, and projection activation drain outstanding activity before proceeding. Loss of the liveness connection prevents later stale transactions from committing.

## Managed indexes and database migrations

Run build, validate/adopt, then retire. Existing index coverage is retained until every replacement validates. Retiring indexes needed by the current plan can be rebuilt/adopted. An invalid managed index is recreated only after its physical definition and identity match the registry; a same-name external replacement is rejected. Recreating an index records its new physical OID.

The relational schema starts with migration V001. Databases initialized with an earlier development revision of V001 must be recreated; there is no migration between those revisions.

PostgreSQL logical restores can change physical OIDs. After a restore, stop ingestion and reconcile managed index identities against actual definitions before allowing retirement. Do not blindly erase identities to force an adoption.

## Reassignments

The subscription includes assignment and unassignment events in both flat and tree stream modes. The `reassignments` SQL view exposes the source and target synchronizers, reassignment ID and counter, submitter, and assignment exclusivity time when supplied. Reassignment updates have no transaction effective time; `effective_at` remains NULL.

A move does not globally archive a contract. Repeated assignments retain one contract identity and payload. If an assignment is the first observation, its embedded created event supplies the payload, but `created_at_offset` stays NULL and `history_lower_bound` is true: the original creation was not observed. A missing template payload fails ingestion explicitly. Reassignment completeness applies only to the subscribed coverage interval and parties; it does not imply knowledge of earlier history or global per-synchronizer availability.

Concurrent observations of the same contract are serialized with transaction advisory locks. PostgreSQL shared lock capacity must accommodate the distinct contracts in concurrently executing batches, especially a single large ledger transaction. Include `max_locks_per_transaction` and ingest concurrency in capacity testing.

Non-causal delivery can expose an archive before its create or assignment. Pending archives survive watermark advances until matched. An archive preceding the earliest observed state produces an empty lifetime, so it cannot resurrect an active contract.

## Pruning and redaction

Stop ingestion before maintenance. Each operation runs in one database transaction and acquires the projection, writer, and activity locks used by the backend. A live writer is rejected. Backfill and projection changes are serialized with maintenance. Use the database credentials and schema of the relational store, typically `--postgres-schema=pqs_relational`; the examples below omit connection options.

```sh
pqs datastore postgres-relational prune --prune-offset=12345
pqs datastore postgres-relational prune --prune-offset=12345 --prune-mode=Force
pqs datastore postgres-relational redact contract --redact-contractid='<contract-id>' --redact-redactionid='<request-id>'
pqs datastore postgres-relational redact exercise --redact-offset=12345 --redact-node=0 --redact-redactionid='<request-id>'
```

Pruning defaults to a dry run reporting row counts. Force removes contracts first observed and archived through the inclusive offset, their payloads and interface rows, and eligible exercise, movement, visibility, event, and transaction history. Active contracts, their original create events, retained lifecycle references, and a restart checkpoint survive. Offsets beyond the published watermark are rejected. Reads below the retained boundary are rejected, including an already configured session offset. Repeating a completed prune is a no-op. Coverage records indicate the loss of historical completeness.

Minimal contract-ID tombstones prevent late assignments from recreating a pruned archived contract. Tombstones and redaction audit records are intentionally retained; include their growth in capacity planning. Pruning does not prune the participant ledger, and this command accepts an offset rather than a time or duration.

Contract redaction requires a published, archived contract. It deletes whole template and interface payload rows, including promoted fields and their index entries; clears the key, key hash and created-event blob; and removes stored exercise arguments and results for the contract. Exercise redaction can instead target a single event by offset and node. Both keep an audit request ID. Repeating the same request is a no-op; a different request for an already redacted item is rejected. Later ingestion and backfill must not restore redacted values.

Redaction retains contract identity, parties, lifecycle and movement metadata. It is payload removal, not anonymization or immediate physical erasure from PostgreSQL pages, WAL, replicas or backups. Their retention must be managed separately. Readers holding older database snapshots can continue seeing their snapshot until it ends.

Maintenance is atomic and currently runs as one transaction, so a large prune needs a maintenance window with sufficient WAL and disk headroom. Chunked online pruning is not implemented. Production-scale performance has not yet been established. Measure representative workloads before choosing a production retention schedule.
