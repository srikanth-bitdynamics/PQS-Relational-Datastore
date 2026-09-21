# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

This release includes the following SQL migrations:
- _postgres-relational/V001__Create_relational_schema.sql_: creates the initial relational schema in its own Flyway history. Does not migrate existing document stores. **[Impact: Instantaneous]**
- _V043__Add_reassignment_event_types.sql_: adds the `assign` and `unassign` labels to the `__event_type` enum. Metadata-only, no table is scanned or rewritten. **[Impact: Instantaneous]**

## What's New

### Experimental relational backend

- Add `postgres-relational` ingestion with retained JSON payloads, selected typed columns and SQL query views.
- Add projection apply, backfill and activation commands, with managed index planning, build, adoption and retirement.
- Record ingestion coverage and provenance, including assignment and unassignment history within the subscribed interval.
- Add relational pruning with a dry run and contract/exercise payload redaction. Maintenance requires stopped ingestion; replay guards prevent removed payloads from being restored.
- HTTP Query API, document-store migration and chunked online pruning are not included.

### Multi-sync support

- PQS now subscribes to reassignments in addition to transactions. Every `Reassignment` received from the ledger is recorded in `__transactions`, and its `Assigned` and `Unassigned` events are recorded in `__events` with the new `assign` and `unassign` types.

### Bug fixes

- Propagate PostgreSQL commit failures before acknowledging ingestion progress.
- Include known implementing-template payloads when subscribing to all interfaces.
