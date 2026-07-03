# Journal batched-write throughput

Measures write throughput of the opt-in `StreamBatchedWriteJournal` against the default `R2dbcJournal` as the number of
concurrently-writing persistent actors increases. This is the scenario the batched journal targets: coalescing writes
from many different persistence ids into one multi-row statement.

## What it measures

`N` persistent actors each persist `eventsCount` events. Because each actor persists sequentially (Pekko serialises a
single actor's writes), the actors march through `eventsCount` rounds in lockstep; in each round the `N` concurrent
writes are what the batched journal coalesces. We time how long it takes for all `N * eventsCount` events to be
persisted, take the best of several iterations, and report events/second.

## Prerequisites

- Docker (for PostgreSQL) and JDK 17.
- A running PostgreSQL with the schema loaded:

  ```shell
  docker compose -f docker/docker-compose-postgres.yml up -d
  docker exec -i docker-postgres-db-1 psql -U postgres -t < ddl-scripts/create_tables_postgres.sql
  ```

## How to replicate

1. Copy the harness into the test source tree (it is intentionally kept out of the build):

   ```shell
   cp benchmarks/journal-batched-write-throughput/StreamBatchedWriteJournalBenchmarkSpec.scala \
      core/src/test/scala/org/apache/pekko/persistence/r2dbc/journal/
   ```

2. Run the sweep (uses JDK 17 via coursier; adjust if your JDK 17 is elsewhere):

   ```shell
   export JAVA_HOME=$(cs java-home --jvm temurin:1.17.0.0)
   sbt -java-home "$JAVA_HOME" \
     -Dpekko.bench.actorCounts=1,10,50,200,500 \
     -Dpekko.bench.eventsCount=20 \
     -Dpekko.bench.iterations=5 \
     -Dpekko.bench.warmup=2 \
     "core/Test/testOnly org.apache.pekko.persistence.r2dbc.journal.BenchmarkDefaultManyActorsSpec org.apache.pekko.persistence.r2dbc.journal.BenchmarkBatchedAddManyActorsSpec org.apache.pekko.persistence.r2dbc.journal.BenchmarkBatchedUnnestManyActorsSpec"
   ```

   All three strategies are measured in one run so the `BENCH[...]` lines are directly comparable:
   `default` (no batching), `add` (batched, `batch.use-unnest = off`), and `unnest` (batched, `batch.use-unnest = on`).
   The result lines are prefixed with `BENCH[...]`; filter with `... 2>&1 | grep BENCH`.

3. Remove the harness from the source tree afterwards so it does not become part of the compiled test suite:

   ```shell
   rm core/src/test/scala/org/apache/pekko/persistence/r2dbc/journal/StreamBatchedWriteJournalBenchmarkSpec.scala
   ```

### Parameters (`-Dpekko.bench.*`)

| property | meaning | default |
|---|---|---|
| `pekko.bench.actorCounts` | comma-separated actor-count sweep | `1,10,50,200,500` |
| `pekko.bench.eventsCount`  | events persisted per actor per iteration | `20` |
| `pekko.bench.iterations`   | measured iterations (best reported) | `5` |
| `pekko.bench.warmup`       | warm-up iterations (not measured) | `2` |
| `pekko.bench.window`       | batched journal `batch.window` (batched specs only) | `10ms` |
| `pekko.bench.unnestMaxPayloadSize` | `batch.unnest-max-payload-size` for the `unnest` spec (`1 GiB` = raw uncapped) | `8 KiB` |
| `pekko.bench.payloadSize`  | event payload size in bytes (`0` = tiny default `BenchActor`) | `0` |

Only `-Dpekko.*` properties reach the forked test JVM (see `project/CommonSettings.scala`), which is why the benchmark
properties use the `pekko.bench.` prefix.

For high actor counts (1k+) raise the forked test JVM heap, e.g. prepend `'set Test/javaOptions += "-Xmx3g"'` before
the `testOnly` command.

Batched-journal tuning (`pekko.persistence.r2dbc.batched-journal.batch.*` - `window`, `max-requests`, `parallelism`)
and `connection-factory.max-size` can be overridden the same way for sensitivity analysis.

### Experimental: UNNEST insert path

The `unnest` spec (`BenchmarkBatchedUnnestManyActorsSpec`) runs the batched journal with an experimental `UNNEST`-based
multi-row insert (`batch.use-unnest = on`): each coalesced batch is written with a single statement carrying one array
parameter per column, instead of an R2DBC `add()` batch that executes the prepared insert once per row. It is only
supported for the postgres/yugabyte dialects with `bytea` payloads (it falls back to the `add()` batch for json/jsonb
payloads). Because the harness runs `default`, `add` and `unnest` in the same session, one sweep tells you directly when
to use no batching, the `add()` batch, or the `UNNEST` batch.

Raw run: [`results/2026-07-02-loopback-postgres-unnest-vs-add.txt`](results/2026-07-02-loopback-postgres-unnest-vs-add.txt).
PostgreSQL 18.4 over loopback, JDK 17, Scala 2.13, 12-CPU host, pool `max-size=20`, batched defaults, `eventsCount=20`,
best of 3, tiny payloads:

| concurrent actors | batched `add()` (ev/s) | batched `UNNEST` (ev/s) | UNNEST vs add() |
|--:|--:|--:|--:|
| 10   | 340    | 342    | 1.01x |
| 50   | 1,621  | 1,675  | 1.03x |
| 200  | 13,055 | 25,301 | 1.94x |
| 500  | 25,737 | 46,665 | 1.81x |
| 1000 | 34,358 | 53,433 | 1.55x |

At >= 200 concurrent actors the `UNNEST` insert is **~1.5-1.9x faster** than the `add()` batch (one execution with
column-oriented array parameters vs one prepared-statement execution per row); at low concurrency the two are within
noise because batches are tiny. Payloads here are tiny, so the `UNNEST` path's hex-encoding of `bytea` payloads adds
negligible bytes - re-measure with realistic payload sizes before generalising.

Two caveats sharpen this (raw run: same file, plus
[`results/2026-07-02-loopback-postgres-unnest-payload-size.txt`](results/2026-07-02-loopback-postgres-unnest-payload-size.txt)):

- **The `UNNEST` win needs the size-flush regime.** Below `batch.max-requests`, rounds flush on the `batch.window` timer
  and `add()` == `UNNEST`; at/above it they flush by size and `UNNEST` pulls ahead. Lowering `max-requests` to the
  concurrency level moves the ~1.9-2.5x win down to ~60-90 actors. So the crossover is governed by `max-requests`, not an
  absolute actor count.
- **`UNNEST` only helps for small events.** Because `bytea` payloads are passed as hex `text[]` (2x the bytes), the
  advantage fades as events grow. Measured three-way at 500 actors (raw run:
  [`results/2026-07-02-loopback-postgres-unnest-payload-size.txt`](results/2026-07-02-loopback-postgres-unnest-payload-size.txt)):

  | payload | default (ev/s) | `add()` (ev/s) | `UNNEST` (ev/s) | winner |
  |--:|--:|--:|--:|:--|
  | 256 B | 8,880 | 23,120 | 52,597 | UNNEST |
  | 1 KB  | 13,320 | 19,178 | 45,545 | UNNEST |
  | 4 KB  | 12,710 | 16,698 | 22,904 | UNNEST |
  | 8 KB  | 9,978 | 11,115 | 11,849 | UNNEST |
  | 16 KB | 6,655 | 8,329 | 6,847 | **add()** |
  | 128 KB | 2,345 | 1,773 | 847 | **default** |

  There are two crossovers: `UNNEST` loses to `add()` above ~8 KB (hex overhead), but stays ~level with the default
  until much larger events; and even `add()` drops below the default for very large events. So at high concurrency
  (loopback): use `UNNEST` up to ~8 KB, `add()` from ~8 KB to tens of KB, and the default (no batching) for very large
  (~100 KB+) events.

- **`UNNEST` is a stability risk for large events - not just slower.** Extending the sweep past 128 KB (raw run:
  [`results/2026-07-02-loopback-postgres-unnest-large-payload.txt`](results/2026-07-02-loopback-postgres-unnest-large-payload.txt))
  showed the `UNNEST` path **fails with `OutOfMemoryError`** (direct buffer memory, then Java heap) at events of ~256 KB
  and above, because the whole hex-encoded batch is materialized as off-heap buffers (memory ~ `max-requests * 2 *
  event-size`). `default` and `add()` survive all sizes. This is why `batch.use-unnest` is applied adaptively.

The `unnest` spec is the **adaptive** journal (`batch.use-unnest = on` with the default `batch.unnest-max-payload-size =
8 KiB`), so a single `use-unnest = on` config picks the best safe insert per batch (raw run:
[`results/2026-07-03-loopback-postgres-adaptive-unnest.txt`](results/2026-07-03-loopback-postgres-adaptive-unnest.txt)):

| payload | default | `add()` | `unnest` (adaptive) | adaptive did |
|--:|--:|--:|--:|:--|
| 1 KB | 4,941 | 7,109 | 22,515 | UNNEST (3.2x add) |
| 16 KB | 4,679 | 6,159 | 6,573 | add() fallback (≈ add) |
| 256 KB | 1,231 | 870 | 1,055 | add() fallback - **completes, no OOM** |

It uses `UNNEST` where it wins (≤ 8 KB) and falls back to `add()` for larger events - so it never does worse than the
`add()` batch and never OOMs. To measure raw uncapped `UNNEST` (e.g. to reproduce the large-payload OOM), run the
`unnest` spec with `-Dpekko.bench.unnestMaxPayloadSize=1GiB`.



## Results

See `results/` for raw runs.

### Actor-count sweep (loopback defaults)

Raw run: [`results/2026-06-25-loopback-postgres.txt`](results/2026-06-25-loopback-postgres.txt).

PostgreSQL 18.4 over loopback, JDK 17, Scala 2.13.18, 12-CPU host, pool `max-size=20`, batched defaults
(`window=10ms`, `max-requests=100`, `parallelism=8`), `eventsCount=20`, best of 5:

| concurrent actors | default (ev/s) | batched (ev/s) | batched vs default |
|--:|--:|--:|--:|
| 1   | 357    | 34     | 0.10x (10x slower) |
| 10  | 3,132  | 340    | 0.11x |
| 50  | 5,311  | 1,721  | 0.32x |
| 200 | 8,579  | 9,768  | 1.14x |
| 500 | 13,343 | 19,588 | 1.47x |

### High-concurrency window sweep (1k-3k actors)

Raw run: [`results/2026-06-25-loopback-postgres-window-sweep.txt`](results/2026-06-25-loopback-postgres-window-sweep.txt).

Same environment as above but with the forked test JVM at `-Xmx3g`, `eventsCount=20`, best of 3. Each `batch.window`
was run in its own session; the default journal is window-independent, so its value is the same-session baseline.

Throughput in ev/s (speedup = batched / default in the same session):

| actors | default | batched 3ms | batched 5ms | batched 10ms |
|--:|--:|--:|--:|--:|
| 1000 | 12.6k-15.7k | 22,588 (1.44x) | 22,679 (1.81x) | 22,134 (1.72x) |
| 2000 | 11.9k-16.1k | 24,728 (1.54x) | 26,016 (2.19x) | 25,020 (1.83x) |
| 3000 | 14.5k-15.6k | 24,003 (1.65x) | 25,875 (1.66x) | 24,548 (1.69x) |

At 1k-3k concurrent actors the batched journal is consistently **~1.4-2.2x faster** than the default, even on loopback.
Observations:

- **Window length barely affects peak throughput here.** With thousands of actors each round already fills batches
  regardless of the window, so 3/5/10 ms land within run-to-run noise (5 ms marginally best). The window mostly trades
  latency, not throughput, at this concurrency.
- **Batched throughput plateaus around 24k-26k ev/s.** The lever here is `batch.parallelism` (x pool size), **not**
  `batch.max-requests`: a separate sweep ([`results/2026-06-26-loopback-postgres-maxrequests-sweep.txt`](results/2026-06-26-loopback-postgres-maxrequests-sweep.txt))
  shows throughput *falls* as `max-requests` grows, because large batches collapse a round onto one `mapAsync` slot and
  under-utilise parallelism. Keep `max-requests` roughly `<= actorCount / parallelism` so all slots stay busy.
- The default journal's spread (~12k-16k) is run-to-run variance (it ignores the window).

## Interpretation

- **Crossover at ~100-200 concurrent actors.** Below it the batched journal is slower; above it, it pulls ahead and the
  gap is still widening at 500 actors (its curve keeps climbing as more entities pack into each window, while the
  default plateaus toward the connection-pool limit).
- **Low concurrency is a net loss** (up to ~10x slower at one actor): a single actor's sequential persists never
  coalesce, so each one just pays the `batch.window` latency. This is the throughput-vs-latency trade-off and is
  expected.
- **Loopback is the pessimistic case.** With near-zero network round-trip time the default journal's single-row inserts
  are cheap, so batching here mainly amortises commit/fsync. Against a **networked** database (millisecond-level RTT per
  round-trip) the default journal's per-write cost is far higher, so the crossover moves to **lower** concurrency and
  the speedup grows. Treat the ~1.5x here as a floor, not a ceiling.
- **Tuning** shifts the curve: a smaller `window` improves latency at little throughput cost at high concurrency;
  `parallelism` (bounded by the connection pool) is the real throughput lever. Raising `max-requests` does **not** help
  (and hurts on loopback) - see the max-requests sweep linked above.

**Bottom line:** beneficial for high-concurrency, many-entity write workloads (especially against a networked DB); not
appropriate for low-concurrency or latency-sensitive single-entity workloads.

## Tuning guidance

Raw data: [`results/2026-06-26-loopback-postgres-maxrequests-sweep.txt`](results/2026-06-26-loopback-postgres-maxrequests-sweep.txt)
(window=2 ms, max-requests in {100, 500, 1000, 5000}, 1k-3k actors).

| actors | default | mr=100 | mr=500 | mr=1000 | mr=5000 |
|--:|--:|--:|--:|--:|--:|
| 1000 | 16,950 | **22,499** | 12,445 | 7,123 | 6,917 |
| 2000 | 17,454 | **25,938** | 18,879 | 12,609 | 8,912 |
| 3000 | 16,368 | **26,357** | 22,902 | 16,818 | 8,594 |

### Why raising `max-requests` hurts (counter-intuitive)

`groupedWithin(max-requests, window)` flushes a batch when **either** limit is hit. So:

- With a **small** `max-requests`, a round of N concurrent writes becomes `ceil(N / max-requests)` batches that fan out
  across `batch.parallelism` slots and the connection pool - many small, pipelined transactions.
- With a **large** `max-requests`, the whole round collapses into **one big batch handled by a single `mapAsync`
  slot**; the other slots sit idle, and one large transaction is slower than many small pipelined ones.

Effective parallelism is therefore `~ min(ceil(N / max-requests), parallelism)`, so a large `max-requests` *starves*
parallelism. (On loopback, where round-trips are nearly free, this dominates; `max-requests` is essentially a safety cap
rather than a throughput lever.)

### Recommendation

- **`window`** - keep it small (the default is `2 ms`). It bounds the extra write latency and costs little throughput at
  high concurrency, where batches fill quickly regardless.
- **`max-requests`** - leave it modest (default `100`); do **not** raise it for throughput. Rule of thumb: keep it
  roughly `<= concurrent-writers / parallelism` so every slot stays busy.
- **`parallelism`** - this is the real throughput lever. Increase it (and `connection-factory.max-size`) for more
  write throughput, but leave headroom in the pool for recovery/queries that share it.
- **Networked databases** - the loopback result favours many small batches because round-trips are almost free. With
  real network latency, fewer/larger batches save round-trips, so the optimum `max-requests` will be higher - re-measure
  for your deployment. Even then, avoid very large values (giant transactions inflate lock time and failure-retry cost).

