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

## Batched writes

The batched write journal is an opt-in alternative to the default journal that coalesces concurrent writes from many
persistent actors into a single multi-row `INSERT` statement. It trades a small amount of write latency (bounded by
`batch.window`) for higher write throughput when many entities are written concurrently. Because Pekko Persistence
already serialises the writes of a single persistent actor, the benefit comes entirely from batching across different
persistence ids.

Enable it by pointing the journal plugin at the `batched-journal` configuration block:

```
pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.batched-journal"
```

It can also be enabled with the `journalPluginId` for a specific `EventSourcedBehavior`. The `batched-journal` block
inherits all settings from the default `journal` block and adds the batching settings below.

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#batched-journal-settings}

### Tuning

Any `batch.*` setting can be overridden per plugin in your `application.conf`, for example:

```
pekko.persistence.r2dbc.batched-journal.batch {
  window      = 1 ms    # even lower latency
  parallelism = 20      # more write throughput; keep <= connection-factory.max-size
}
```

* `batch.window` - upper bound on the extra write latency; the stream waits up to this long to coalesce writes. The
  default is small (`2 ms`); lower it for latency-sensitive writes. Raising it only helps if writes arrive too sparsely
  to fill batches, and at high concurrency it has little effect on throughput.
* `batch.parallelism` - number of batches written to the database concurrently. **This is the main throughput lever.**
  Raise it (together with `connection-factory.max-size`) for more write throughput, but leave pool headroom for
  recovery and queries that share the same connection pool; there is no benefit in setting it higher than
  `connection-factory.max-size`.
* `batch.max-requests` - maximum number of writes coalesced into one multi-row `INSERT`. This is mainly a safety cap,
  **not** a throughput lever: making it much larger collapses a burst onto fewer, larger transactions that under-use
  `parallelism` and *lower* throughput. Keep it roughly `<= concurrent-writers / parallelism`. (Against a networked
  database, where round-trips are costlier, a somewhat larger value can help - measure for your deployment.)
* `batch.queue-size` - capacity of the bounded buffer in front of the stream. Raise it to absorb larger write bursts;
  on overflow, writes are rejected (the originating persistent actor sees the write fail) rather than blocking.
* `batch.use-unnest` (experimental, default `off`) - insert each coalesced batch with a single `UNNEST`-based multi-row
  statement (one execution with one array parameter per column) instead of an R2DBC `add()` batch (one prepared-statement
  execution per row). Only supported for the `postgres`/`yugabyte` dialects with `bytea` payloads; with `json`/`jsonb`
  payloads it is ignored and the `add()` batch is used. When on it is applied *adaptively* per batch (see
  `batch.unnest-max-payload-size`). Measure before enabling - the benefit depends on the workload and database. Two
  effects dominate:
    * It only helps once batches actually flush by *size* (roughly when concurrent writers `>= batch.max-requests`); at
      lower concurrency writes flush on the `batch.window` timer and the insert method makes no difference.
    * Because `bytea` payloads are passed as hex `text[]` (doubling those bytes on the wire), the benefit shrinks as
      events grow and inverts for large events. Measured three-way at high concurrency on loopback: `UNNEST` was fastest
      up to ~8 KB per event (up to ~2.3x over the `add()` batch and ~6x over the default journal); from ~8 KB the `add()`
      batch was fastest; and for very large events (~100 KB+) even the `add()` batch fell below the default journal, so
      *not batching* won. The `batch.unnest-max-payload-size` cap keeps `UNNEST` to the small events where it wins and
      routes larger events to the `add()` batch automatically.
* `batch.unnest-max-payload-size` (default `8 KiB`) - per-event payload-size cap for `UNNEST`. When `batch.use-unnest`
  is on, a coalesced batch uses `UNNEST` only if every event's serialized payload (event + metadata) is at most this
  size; a batch containing a larger event falls back to the `add()` batch. This bounds `UNNEST`'s off-heap memory use
  (roughly `batch.max-requests * 2 * event-size`) so a large event cannot OOM the writer - in testing, events of
  ~256 KB and above written via an *uncapped* `UNNEST` reproducibly failed with an `OutOfMemoryError`. The default keeps
  `UNNEST` in its measured throughput sweet spot; raise it only if you have verified the memory headroom for larger
  events.

The batched journal only helps when many different persistence ids are written concurrently. For low write concurrency,
or for latency-sensitive single-entity writes, prefer the default journal - the added `batch.window` latency is not
offset by coalescing. Always measure throughput for your own workload and database; the benefit is larger against a
networked database than over a local loopback connection.

### Requirements and limitations

* Only the `postgres` and `yugabyte` dialects are supported.
* `use-app-timestamp` and `db-timestamp-monotonic-increasing` must be `on` (the `batched-journal` block enables them).
  Because the events in a batch belong to different persistence ids the per-persistence-id timestamp subselect of the
  default journal cannot be used, so the timestamps are generated by the application instead.
* These requirements are checked when the journal starts; misconfiguration fails fast.

### Failure isolation

The events of a coalesced batch are written in a single transaction. If that write fails - for example because one of
the persistent actors in the batch attempted to write a duplicate sequence number - the batch is automatically retried
one request at a time, so that only the genuinely failing write fails and the other writes in the same batch still
succeed.

### Overload

The buffer in front of the batching stream is bounded by `batch.queue-size`. If it overflows (the database cannot keep
up with the write load) new writes are rejected and the originating persistent actor sees the write fail, rather than
blocking indefinitely.

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
