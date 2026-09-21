# PostgreSQL relational datastore

The relational datastore exports ledger data to PostgreSQL. It stores contract payloads as JSONB and exposes selected Daml fields as typed SQL columns. Applications query PostgreSQL directly.

A projection defines which template fields become SQL columns. Applying a projection creates a draft; backfill populates its columns from stored payloads; activation publishes its query views. Ingestion then maintains the active projection.

## Prerequisites

- A PQS build that includes the `postgres-relational` target. Check with `pqs pipeline --help`. For building PQS from source, see [Engineer Setup](../../../ENGINEER_SETUP.md).
- A PostgreSQL database and credentials with permission to create the datastore schema and its objects.
- A participant Ledger API endpoint and access to the packages and parties to be ingested.
- The Daml packages containing the templates used by the projection, uploaded to the participant.

The commands below use `pqs`. If running a jar directly, replace it with `java -jar /path/to/pqs.jar`.

Use a separate PostgreSQL schema for each datastore. The examples use `pqs_relational`; do not point them at an existing document datastore schema. There is no document-to-relational migration command.

## Configuration

Copy [examples/relational.conf](examples/relational.conf) to `relational.conf` and update the Ledger API and database connection settings. Set `RELATIONAL_DB_PASSWORD` in the environment before running the commands.

The example uses `NoAuth` for a local participant without authentication. For an authenticated participant, set `ledger.auth = OAuth` and configure `oauth` with an access token or client credentials. Ledger TLS settings belong under `ledger.tls`; PostgreSQL TLS settings belong under `postgres.tls`.

The same file supports ingestion and datastore commands:

| Setting | Datastore commands | Ingestion |
| --- | --- | --- |
| Ledger connection | `ledger` | `source.ledger` |
| Authentication | `oauth` | `pipeline.oauth` |
| PostgreSQL connection | `postgres` | `target.postgres` |
| Contract filter | `filter.contracts` for schema commands | `pipeline.filter.contracts` |

The example uses HOCON substitutions to share these settings. Configure the connection values in the file so both command families use the same database and participant.

Replace `Finance:Main:Asset` with a template from your packages. The identifier is `package-name:module-name:template-name`, not a package ID. Replace `owner` and `amount` with fields from that template:

```hocon
projections {
  assets {
    templates = ["Finance:Main:Asset"]
    promote = ["owner", "amount"]
    queries = [{ filter = ["owner"], order = ["amount desc"] }]
  }
}
```

`promote` selects top-level fields for SQL columns. Scalar fields, enums and supported optional scalar fields can be promoted when their definitions are compatible across package versions. Unsupported fields remain in JSON; projection apply reports diagnostics for requested fields it cannot promote. Typed template projections do not flatten nested records, lists or maps.

`queries` describes equality filters and ordering for index planning. It does not filter the ingested contracts or execute a query. The example requests an index for filtering by owner and ordering by amount.

## Initial setup

Run these steps with no other relational ingestion process using the schema.

### 1. Create the schema

```sh
pqs datastore postgres-relational schema apply --config relational.conf
```

This applies database migrations and registers the templates, interfaces and choices found in the package metadata. It does not ingest contracts.

### 2. Ingest through the current ledger end

```sh
pqs pipeline ledger postgres-relational --config relational.conf --pipeline-ledger-stop=Latest
```

Wait for the command to finish. This gives the projection a fixed checkpoint to backfill through.

The example uses `Oldest` as the start position. On an empty datastore, it starts from the participant's available beginning, seeding active contracts where necessary. On subsequent runs, it resumes from the stored checkpoint. Data before the available history is not recovered by an active-contract snapshot.

The default `TransactionStream` does not provide exercise history. If exercise history is needed, set `pipeline.datasource = TransactionTreeStream` before the first ingestion. Changing it later does not recover previously omitted exercises.

### 3. Apply, backfill and activate the projection

```sh
pqs datastore postgres-relational projection apply --config relational.conf
pqs datastore postgres-relational projection backfill --config relational.conf
pqs datastore postgres-relational projection activate --config relational.conf
pqs datastore postgres-relational projection list --config relational.conf
```

Check the apply diagnostics and confirm that the version is `active`. Backfill and activation operate on the latest draft. Activation requires ingestion to be stopped and backfill to cover the published checkpoint.

### 4. Start continuous ingestion

```sh
pqs pipeline ledger postgres-relational --config relational.conf
```

The configuration uses `stop = Never`, so this process continues following ledger updates. Only one relational writer may use the schema at a time.

## Query the datastore

Connect a PostgreSQL client to the configured database. Set its search path and check ingestion progress:

```sql
SET search_path TO pqs_relational;
SELECT * FROM latest_checkpoint();
SELECT contract_id, created_at_offset FROM active_contracts LIMIT 20;
```

The checkpoint contains the published ledger offset and PQS transaction index. An empty result means that no checkpoint has been published yet.

During setup, look up the generated view name for the template:

```sql
SELECT query_view
FROM pqs_relational.__rel_entity
WHERE package_name = 'Finance'
  AND module_name = 'Main'
  AND entity_name = 'Asset'
  AND kind = 'template';
```

Replace `VIEW_NAME` below with that result. Names may contain a hash suffix, so do not construct them from the template name.

```sql
SELECT contract_id, owner, amount
FROM pqs_relational."VIEW_NAME"
WHERE amount > 50
ORDER BY amount DESC
LIMIT 20;
```

Activated `q_` views expose the promoted fields and `payload_json`. They return active, non-redacted, non-divulged contracts at the selected checkpoint. By default, this is the latest published checkpoint. For a historical snapshot, call `set_latest` with a retained ledger offset in the same connection before querying the view.

Use `transactions` for transaction metadata and `reassignments` for assignment and unassignment events. These views respect the selected offset range. The document backend's `active()` and `creates()` functions are not the relational query interface.

Application queries should use the published views. The `__rel_entity` lookup above is for schema discovery; tables prefixed with `__` and the physical `rel_` payload tables are implementation details. Configure PostgreSQL privileges for application readers separately from the Ledger API credentials used by ingestion.

## Managed indexes

After activating a projection, inspect its index plan, build the indexes and adopt them:

```sh
pqs datastore postgres-relational projection index plan --config relational.conf
pqs datastore postgres-relational projection index build --config relational.conf
pqs datastore postgres-relational projection index adopt --config relational.conf
pqs datastore postgres-relational projection index list --config relational.conf
```

The plan reports the portion of each query covered by payload-table indexes. Build creates indexes concurrently and validates them. Adoption retains existing coverage until all planned replacements are valid. To remove indexes marked for retirement:

```sh
pqs datastore postgres-relational projection index retire --config relational.conf
```

## Changes and maintenance

For a projection change, stop ingestion, update the configuration, and repeat apply, backfill and activate before restarting ingestion. If packages have changed, run schema apply first. A failed backfill retains its committed progress and can be retried.

Adding compatible columns preserves existing query views and their grants. Removing a published field or template requires an explicit consumer migration. Package changes can also affect whether an existing promoted field remains compatible.

See [Relational projection operations](OPERATIONS.md) for upgrade compatibility, writer recovery, reassignment semantics, pruning and payload redaction. It also describes the V001 development-schema recreation requirement and the current limits of maintenance operations.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Template not found during projection apply | Use the package name, module and template name from the uploaded package. |
| Requested field remains in JSON | Read the apply diagnostics; check its type and compatibility across package versions. |
| Activation reports a live writer | Stop ingestion, wait for it to exit, and retry activation. |
| Activation reports an older backfill checkpoint | Keep ingestion stopped, rerun backfill, then activate. |
| Generated query view does not exist | Confirm that the projection includes the template and has been activated. Schema apply alone does not publish it. |
| Query returns no contracts | Check the checkpoint, ingestion filters and Ledger API rights, and whether the contracts are still active. |

For available options, run `pqs pipeline --help` or append `--help` to the relevant datastore command.
