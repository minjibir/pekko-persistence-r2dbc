# Journal plugin

The journal plugin enables storing and loading events for @extref:[event sourced persistent actors](pekko:typed/persistence.html).

## Schema

The `event_journal` table and `event_journal_slice_idx` index need to be created in the configured database, see schema definition in @ref:[Creating the schema](getting-started.md#schema).

The `event_journal_slice_idx` index is only needed if the slice based @ref:[queries](query.md) are used.

## Relation to Pekko JDBC plugin

Pekko Persistence R2DBC plugin tables are not compatible with the tables of Pekko Persistence JDBC. JDBC data must be migrated using the @ref:[migration tool](migration.md) and a different schema/database must be used (or the table names overridden). 

## Configuration

To enable the journal plugin to be used by default, add the following line to your Pekko `application.conf`:

```
pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.journal"
```

It can also be enabled with the `journalPluginId` for a specific `EventSourcedBehavior` and multiple
plugin configurations are supported.

See also @ref:[Configuration](config.md).

### Reference configuration 

The following can be overridden in your `application.conf` for the journal specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#journal-settings}

## Batched Journal

The batched journal plugin (`R2dbcBatchJournal`) coalesces concurrent write requests across multiple persistence IDs into shared multi-row database operations to increase throughput under high concurrency.

### Batched Journal Configuration

To enable the batched journal, update `application.conf`:

```
pekko.persistence.r2dbc {
  # Batched journal requires application timestamps and monotonic database timestamps
  use-app-timestamp = on
  db-timestamp-monotonic-increasing = on

  journal {
    class = "org.apache.pekko.persistence.r2dbc.journal.R2dbcBatchJournal"
    max-batch-size = 100
    max-batch-time = 2ms
  }
}
```

The batched journal uses the following settings:

- `max-batch-size`: The maximum number of writes to coalesce into a single database insert batch.
- `max-batch-time`: The maximum time to buffer writes before flushing the batch to the database.

## Deletes

The journal supports deletes through hard deletes, which means the journal entries are actually deleted from the database. 
There is no materialized view with a copy of the event so make sure to not delete events too early if they are used from projections or queries.

For each persistent id one tombstone record is kept in the event journal when all events of a persistence id have been
deleted. The reason for the tombstone record is to keep track of the latest sequence number so that subsequent events
don't reuse the same sequence numbers that have been deleted.

See the @ref[EventSourcedCleanup tool](cleanup.md#event-sourced-cleanup-tool) for more information about how to delete
events, snapshots and tombstone records.

## Event serialization

The events are serialized with @extref:[Pekko Serialization](pekko:serialization.html) and the binary representation
is stored in the `event_payload` column together with information about what serializer that was used in the
`event_ser_id` and `event_ser_manifest` columns.

For PostgreSQL the payload is stored as `BYTEA` type. Alternatively, you can use `JSONB` column type as described in
@ref:[PostgreSQL JSON](postgres_json.md).
