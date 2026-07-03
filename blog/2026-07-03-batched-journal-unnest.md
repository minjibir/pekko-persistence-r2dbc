# Batching, `UNNEST`, and Knowing When *Not* To: A Journey Through Apache Pekko Persistence R2DBC Write Throughput

*How a simple question — "does our batching use `UNNEST`?" — turned into a prototype, a pile of benchmarks, an `OutOfMemoryError`, and a crash-safe adaptive insert strategy.*

---

## TL;DR

- The batched write journal coalesces concurrent writes from many persistent actors into **one multi-row `INSERT`** using R2DBC's `Statement.add()` batching — **not** PostgreSQL `UNNEST`.
- We prototyped an `UNNEST`-based insert (one execution with column-oriented array parameters). On loopback PostgreSQL it is **~1.5–2.9× faster** than the `add()` batch for **small events at high concurrency**.
- But `UNNEST` has to hex-encode `bytea` payloads (the driver can't bind `bytea[]`), which **doubles the bytes** and materializes the whole batch in off-heap buffers. For large events this makes it slower, and at **≥256 KB it reproducibly crashes the writer with an `OutOfMemoryError`.**
- So we made the switch **adaptive**: `UNNEST` for events under a size cap, automatic fallback to `add()` above it. One config, fast where it wins, safe everywhere.
- Along the way we mapped the full **decision surface**: the single-row insert (no batching) beats batching at **low concurrency** *and* at **large event sizes**; batching wins in the box between.

Everything below is reproducible; raw runs are linked from each section.

---

## Table of contents

1. [The setup](#1-the-setup)
2. [Assumptions](#2-assumptions)
3. [Disclaimer](#3-disclaimer)
4. [The question: how does batching actually insert?](#4-the-question-how-does-batching-actually-insert)
5. [Prototyping the `UNNEST` insert](#5-prototyping-the-unnest-insert)
6. [Does it even work? Correctness first](#6-does-it-even-work-correctness-first)
7. [Benchmark methodology](#7-benchmark-methodology)
8. [Finding 1: concurrency — and the `max-requests` regime](#8-finding-1-concurrency--and-the-max-requests-regime)
9. [Finding 2: payload size — a three-way race](#9-finding-2-payload-size--a-three-way-race)
10. [Finding 3: the `UNNEST` cliff — an `OutOfMemoryError`](#10-finding-3-the-unnest-cliff--an-outofmemoryerror)
11. [The amicable fix: an adaptive switch](#11-the-amicable-fix-an-adaptive-switch)
12. [Where the single insert beats batching](#12-where-the-single-insert-beats-batching)
13. [Translating to production system load](#13-translating-to-production-system-load)
14. [Lessons learned](#14-lessons-learned)
15. [Conclusion & future work](#15-conclusion--future-work)
16. [Appendix: reproduce it yourself](#16-appendix-reproduce-it-yourself)

---

## 1. The setup

[Apache Pekko Persistence R2DBC](https://pekko.apache.org/docs/pekko-persistence-r2dbc/current/) is an event-sourcing journal that stores events in a relational database over the reactive R2DBC driver. The default journal (`R2dbcJournal`) writes one persistent actor's events per statement.

This repository also ships an **opt-in `StreamBatchedWriteJournal`**. Because Pekko already serialises a single actor's writes, the only way to get more write throughput is to batch *across* different persistence ids. The batched journal does exactly that: it pushes each write onto a bounded queue, groups concurrent writes with `groupedWithin(max-requests, window)`, and writes each group as **one multi-row `INSERT` in one transaction**. If a group fails (e.g. a duplicate sequence number), it bisects the group and retries so only the genuinely bad write fails.

The knobs (`pekko.persistence.r2dbc.batched-journal.batch.*`):

| setting | meaning | default |
|---|---|---|
| `window` | max time to coalesce before flushing | `2 ms` (`10 ms` in benchmarks) |
| `max-requests` | max writes coalesced into one statement | `100` |
| `parallelism` | batches written concurrently (≤ pool size) | `8` |
| `queue-size` | bounded buffer in front of the stream | `10000` |

**Benchmark environment (used throughout):** PostgreSQL 18.4 in Docker over **loopback**, JDK 17.0.19 (Temurin), Scala 2.13, a 12-CPU / 62 GB host, connection pool `max-size = 20`, batched defaults `window = 10 ms`, `max-requests = 100`, `parallelism = 8`. Throughput is events/second, best of N iterations after warm-up. The workload: **N persistent actors each persist a fixed number of events**; because each actor persists sequentially, the actors advance in lockstep "rounds", and in each round the N concurrent writes are what the batched journal coalesces.

---

## 2. Assumptions

- **The batched journal's own requirements:** application-generated, monotonically increasing timestamps (`use-app-timestamp = on`, `db-timestamp-monotonic-increasing = on`) and the **PostgreSQL or YugabyteDB** dialects. These exist because a coalesced batch mixes many persistence ids, so the default journal's per-persistence-id timestamp subselect can't apply.
- **`bytea` payloads** (the default `payload-column-type = BYTEA`), not `JSON`/`JSONB`.
- **The benchmark models a specific shape of load:** many entities writing concurrently in lockstep rounds. Real traffic is burstier and less synchronized.
- **"Concurrency" means concurrent *distinct* persistence ids.** A single hot aggregate cannot self-coalesce.

---

## 3. Disclaimer

Please read this before quoting any number:

- **This is a prototype**, explored on a feature branch. It is *not* a merged or production-hardened feature.
- **All measurements are loopback**, single-machine, PostgreSQL 18.4, specific JDK/Scala versions. **Loopback is the pessimistic case for batching** (near-zero network round-trip time). A networked database shifts every crossover *in batching's favour*.
- Numbers are **directional micro-benchmarks** (best-of-N with real run-to-run variance), not audited results. Some low-sample points are explicitly noisy and flagged as such.
- The `OutOfMemoryError` we hit is tied to the JVM's default `MaxDirectMemorySize` (256 MB) and the specific batch sizes; the *threshold* will move with configuration, but the *mechanism* is real.
- The hex-encoding of payloads is an **artifact of a driver limitation** (`r2dbc-postgresql` can't bind `bytea[]`), not a fundamental property of `UNNEST`. A future binary array binding would change the payload story.
- Your mileage *will* vary. Measure your own workload against your own database.

---

## 4. The question: how does batching actually insert?

It started with a plain question: **does the batched journal use `UNNEST`?**

A quick search settled it — no `UNNEST` anywhere. The batched insert is a single-row prepared statement, bound once per row with `Statement.add()` between rows:

```scala
// insertEventBatchSql — a plain single-row INSERT
INSERT INTO event_journal (slice, entity_type, persistence_id, seq_nr, ...)
VALUES (?, ?, ?, ?, ...)

// executed as a driver batch: bind row 0, add(), bind row 1, add(), ...
r2dbcExecutor.updateInBatch(...)(conn =>
  events.zipWithIndex.foldLeft(conn.createStatement(insertEventBatchSql)) {
    case (stmt, (row, i)) => if (i != 0) stmt.add(); bind(stmt, row)
  })
```

That is *N* executions of one prepared statement inside one transaction. The obvious alternative on PostgreSQL is `UNNEST`: pass **one array per column** and let the server expand them into rows in **a single execution**:

```sql
INSERT INTO event_journal (slice, entity_type, persistence_id, seq_nr, ...)
SELECT * FROM unnest($1::int[], $2::text[], $3::text[], $4::bigint[], ...)
```

Fewer executions, less protocol chatter, one plan. Worth a prototype.

---

## 5. Prototyping the `UNNEST` insert

Three columns made this more interesting than "swap the SQL":

1. **`tags` is `text[]` per row.** You cannot pass "one `text[]` per row" as a parameter array, because PostgreSQL multidimensional arrays must be rectangular and `unnest` flattens them. **Solution:** encode each row's tags as a **JSON array string**, pass a `text[]` of those, and rebuild per row in SQL with `jsonb_array_elements_text`.
2. **`bytea` payloads.** `r2dbc-postgresql` (1.1.x) doesn't support binding `bytea[]`. **Solution:** hex-encode each payload to a `text[]` element and `decode(?, 'hex')` in SQL. (This is the fateful decision — remember it.)
3. **Timestamps.** To dodge any `Instant[]` codec questions, pass ISO-8601 `text[]` and cast `::timestamptz`.

The resulting statement:

```sql
INSERT INTO event_journal (slice, entity_type, persistence_id, seq_nr, writer,
  adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags,
  meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp)
SELECT d.slice, d.entity_type, d.persistence_id, d.seq_nr, d.writer, d.adapter_manifest,
  d.event_ser_id, d.event_ser_manifest,
  decode(d.event_payload_hex, 'hex'),
  CASE WHEN d.tags_json IS NULL THEN NULL
       ELSE ARRAY(SELECT jsonb_array_elements_text(d.tags_json::jsonb)) END,
  d.meta_ser_id, d.meta_ser_manifest,
  CASE WHEN d.meta_payload_hex IS NULL THEN NULL ELSE decode(d.meta_payload_hex, 'hex') END,
  d.db_timestamp::timestamptz
FROM unnest(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  AS d(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest,
       event_ser_id, event_ser_manifest, event_payload_hex, tags_json,
       meta_ser_id, meta_ser_manifest, meta_payload_hex, db_timestamp)
```

It runs in a transaction (so the bisection/rollback semantics are preserved), stays behind an opt-in flag (`batch.use-unnest`), and only engages for the PostgreSQL/YugabyteDB + `bytea` combination — otherwise it transparently falls back to `add()`.

---

## 6. Does it even work? Correctness first

Before any stopwatch, correctness. Against real PostgreSQL, we asserted the `UNNEST` path:

- writes multiple persistence ids in one batch and reads them back with **tags, metadata, and payloads intact** (hex and JSON round-trips verified);
- produces **byte-for-byte identical stored rows** to the `add()` path;
- **rolls back the whole batch** atomically on a duplicate sequence number (bisection retry unaffected);
- replays correctly end-to-end through the journal under concurrent persistent actors.

All green. Only *then* did we start measuring.

---

## 7. Benchmark methodology

A single harness compiles three specs so one run yields directly comparable lines:

- `default` — the `R2dbcJournal` (no batching)
- `add` — batched journal, `batch.use-unnest = off`
- `unnest` — batched journal, `batch.use-unnest = on`

Parameters come from `-Dpekko.bench.*` system properties (actor-count sweep, events per actor, iterations, warm-up, `window`, `max-requests`, a per-event `payloadSize`, and the `UNNEST` size cap). Output lines are prefixed `BENCH[...]`.

---

## 8. Finding 1: concurrency — and the `max-requests` regime

Sweeping concurrent actors (tiny events, `max-requests = 100`) revealed something non-obvious. Batching does **nothing** until concurrency approaches `max-requests`:

| concurrent actors | default | `add()` | `unnest` |
|--:|--:|--:|--:|
| 50 | 8,361 | 1,645 | 1,674 |
| 60 | 7,668 | 2,055 | 2,006 |
| 70 | 10,718 | 2,342 | 2,384 |
| 80 | 13,529 | 2,636 | 2,688 |
| 90 | 16,812 | 2,615 | 3,054 |
| **100** | 11,292 | 6,002 | **16,228** |
| **200** | 12,441 | 11,848 | **34,457** |

Below 100 actors, `add()` and `unnest` are *identical* (~2–3k ev/s) and far below `default`. Then at 100 they leap. The discontinuity lines up exactly with `max-requests = 100`:

- **Below `max-requests`:** each round of writes doesn't fill a batch, so `groupedWithin` flushes on the **`window` timer**. The run is *window-latency-bound* and the insert method is irrelevant.
- **At/above `max-requests`:** batches flush **by size**, the DB insert cost dominates, and `UNNEST`'s single execution pulls ahead.

We confirmed the mechanism by lowering `max-requests` to 50 — the win moved down to 60–90 actors:

| actors | `add()` (mr=100) | `add()` (mr=50) | `unnest` (mr=50) |
|--:|--:|--:|--:|
| 60 | 2,055 | 4,065 | **7,784** |
| 90 | 2,615 | 6,183 | **15,647** |

**Takeaway:** the `UNNEST` benefit isn't tied to a magic actor count; it needs the *size-flush regime* (`concurrent writers ≥ max-requests`). Tune `max-requests` to your concurrency and the batched journal breaks even with `default` at ~60 writers instead of ~200. At genuinely high concurrency, `UNNEST` sustains **~1.5–1.9×** over `add()`:

| actors | `add()` | `unnest` | speedup |
|--:|--:|--:|--:|
| 200 | 13,055 | 25,301 | 1.94× |
| 500 | 25,737 | 46,665 | 1.81× |
| 1000 | 34,358 | 53,433 | 1.55× |

*Raw runs: [actor-count / max-requests crossover](../benchmarks/journal-batched-write-throughput/results/2026-07-02-loopback-postgres-unnest-vs-add.txt).*

---

## 9. Finding 2: payload size — a three-way race

Fixing 500 actors (comfortably in the size-flush regime) and sweeping the event size gives the money chart:

| payload | default | `add()` | `unnest` | winner |
|--:|--:|--:|--:|:--|
| 256 B | 8,880 | 23,120 | **52,597** | unnest (2.3× add, 5.9× default) |
| 1 KB | 13,320 | 19,178 | **45,545** | unnest |
| 4 KB | 12,710 | 16,698 | **22,904** | unnest |
| 8 KB | 9,978 | 11,115 | **11,849** | unnest (break-even vs add) |
| 16 KB | 6,655 | **8,329** | 6,847 | **add()** |
| 32 KB | 4,572 | 4,328 | 4,942 | ~tie |
| 64 KB | **3,049** | 1,509 | 1,813 | **default** |
| 128 KB | **1,823** | 1,723 | 871 | **default** |

Three crossovers hide in that table:

1. **`UNNEST` vs `add()` ≈ 8 KB.** Below it the round-trip/execution savings dominate; above it the hex-doubling costs more than it saves.
2. **`add()` vs `default` ≈ 32–64 KB.** Batching huge payloads into one transaction inflates transaction size more than it saves round-trips (on loopback), so *not batching* wins.
3. **`UNNEST` vs `default` ≈ tens of KB** — `UNNEST` stays level with `default` a bit longer than `add()` does, then falls behind hard.

*Raw runs: [payload-size three-way](../benchmarks/journal-batched-write-throughput/results/2026-07-02-loopback-postgres-unnest-payload-size.txt).*

So far, so tidy: use `UNNEST` for small events, `add()` for medium, `default` for large. Then we pushed past 128 KB.

---

## 10. Finding 3: the `UNNEST` cliff — an `OutOfMemoryError`

Beyond ~150 KB, `UNNEST` didn't just get slower — it **crashed**:

| payload | default | `add()` | `unnest` |
|--:|--:|--:|:--|
| 128 K | 1,823 | 1,723 | 871 (2× slower, completes) |
| 150 K | 1,530 | 1,457 | 615 (completes, shaky) |
| 256 K | 1,014 | 822 | **💥 OOM — direct buffer memory** |
| 300 K | 513* | 748 | **💥 OOM — direct buffer memory** |
| 500 K | 523 | 417 | **💥 OOM — direct buffer memory** |
| 512 K | 530 | 254 | **💥 OOM — Java heap** |

```
java.lang.OutOfMemoryError: Cannot reserve 37748736 bytes of direct buffer memory
  (allocated: 256451228, limit: 268435456)
```

**Why:** the `UNNEST` insert hex-encodes each payload (2×) and materializes the *entire coalesced batch* as one statement's array parameters in **off-heap Netty buffers**. A batch of up to `max-requests` rows × `2 × payload` blows past the 256 MB direct-memory limit. Raising `-Xmx` doesn't help — it's off-heap. `default` (single row) and `add()` (streams per-row bind params, no hex) never build that giant buffer, so they survive every size.

This flipped the framing entirely: for large events, `UNNEST` is not a performance trade-off, it's a **stability liability**. And in real event sourcing, a single oversized event (someone inlines a document, a base64 image, a fat aggregate snapshot) shouldn't be able to OOM the writer.

*Raw run: [large-payload stability](../benchmarks/journal-batched-write-throughput/results/2026-07-02-loopback-postgres-unnest-large-payload.txt).*

---

## 11. The amicable fix: an adaptive switch

The naive fixes are both bad: catching the OOM is unsafe (an off-heap `OutOfMemoryError` isn't `NonFatal`), and "just tell users not to enable it for large events" pushes a footgun onto operators. The **amicable** fix is to **prevent** the condition with a single, intuitive knob.

**Design principles:**

- **Prevent, don't catch** — decide *before* dispatch.
- **One knob for both goals** — a per-event payload-size cap simultaneously keeps `UNNEST` in its performance sweet spot *and* bounds its off-heap memory (a batch is capped at `max-requests` events → worst-case ≈ `max-requests × 2 × cap`).
- **Per-batch, atomic** — a coalesced batch is one statement, so if *any* event exceeds the cap, the whole batch takes the `add()` path.
- **Decision in the journal**, pure/testable helper; the DAO stays dumb.

The core is tiny and readable:

```scala
private[r2dbc] def rowPayloadSize(row: SerializedJournalRow): Int =
  row.payload.fold(0)(_.length) + row.metadata.fold(0)(_.payload.length)

private[r2dbc] def unnestEligible(events: Seq[SerializedJournalRow], maxPayloadSize: Long): Boolean =
  events.forall(rowPayloadSize(_) <= maxPayloadSize)
```

```scala
val useUnnest = useUnnestEnabled &&
  StreamBatchedWriteJournal.unnestEligible(events, batchSettings.unnestMaxPayloadSize)
if (useUnnest) journalDao.writeEventsInBatchUnnest(events)
else           journalDao.writeEventsInBatch(events)
```

A new config key, adaptive by default:

```hocon
use-unnest = off
unnest-max-payload-size = 8 KiB   # UNNEST only when every event's payload <= this, else add()
```

With `max-requests = 100` and an 8 KiB cap, the worst-case `UNNEST` off-heap footprint is ≈ **1.6 MB** — nowhere near the 256 MB limit that the *uncapped* path blew. `use-unnest = on` is now **safe by default**.

**The proof.** The same benchmark spec that OOM'd at 256 KB — now adaptive — sails through in one run:

| payload | default | `add()` | `unnest` (adaptive) | what it did |
|--:|--:|--:|--:|:--|
| 1 KB | 4,941 | 7,109 | **22,515** | used `UNNEST` (3.2× add) |
| 16 KB | 4,679 | 6,159 | 6,573 | fell back to `add()` |
| 256 KB | 1,231 | 870 | **1,055 — no OOM** | fell back to `add()`, **completed** |

Fast where `UNNEST` wins, safe everywhere else, no per-workload tuning.

*Raw run: [adaptive demonstration](../benchmarks/journal-batched-write-throughput/results/2026-07-03-loopback-postgres-adaptive-unnest.txt).*

---

## 12. Where the single insert beats batching

Stepping back, the plain single-row insert (no batching) wins at **two opposite extremes**:

**Low concurrency** — batching needs `concurrent writers ≥ max-requests` to fill batches. Below that, the `window` timer dominates and the single insert is far ahead (see §8). Crossover on loopback: `UNNEST` overtakes `default` at ~100 writers, `add()` at ~200 (both scale down with `max-requests`).

**Large events** — at high concurrency the single insert overtakes batching at **~32 KB** per event and wins decisively from **~64 KB** up (see §9).

The batched journal's sweet spot is therefore a **box**:

```
                batched wins                         single insert wins
  ┌──────────────────────────────────────┐
  │  concurrency ≳ max-requests           │   • concurrency below that      (underloaded)
  │            AND                        │   • OR event size ≳ 32–64 KB     (big events)
  │  event size ≲ ~32 KB                  │
  └──────────────────────────────────────┘
```

---

## 13. Translating to production system load

Loopback is the pessimistic case. On a **networked database** each single-row insert pays a full **round-trip** and often a **commit/fsync**, and those are exactly the costs batching collapses by a factor of the batch size. So in production, the concurrency crossover moves *down* (plausibly ~10–20 concurrent writers) and batching's advantage grows.

**Which resource each strategy loads (per coalesced write of B events):**

| resource | default (single) | `add()` batch | `UNNEST` batch |
|---|---|---|---|
| network round-trips | 1 **per event** | 1 per batch | 1 per batch |
| commit / WAL fsync | 1 per event | **1 per batch** | 1 per batch |
| statement parse/plan | 1 per event | B executes | **1 execute** |
| client CPU | serialize | + B binds | + **hex-encode (∝ size)** |
| heap / off-heap memory | low | moderate | **high, off-heap ∝ B×size×2** ⚠ |
| network bytes | payload | payload | **2× payload (hex)** |
| DB connections held | **1 per concurrent write** | `parallelism` | `parallelism` |
| rows / index writes | B | B | B (batching never reduces this) |

Two production levers deserve emphasis beyond raw throughput:

- **Connection-pool decoupling.** `default` holds a connection *per concurrent writer*; 1,000 hot entities pressure `max_connections`. Batched funnels everything through `batch.parallelism` connections regardless of writer count. This often matters more than ev/s — it protects the database from connection exhaustion.
- **Commit/fsync amortization.** One commit per batch instead of per event means far fewer WAL fsyncs — less disk-I/O pressure and friendlier behaviour under `synchronous_commit`/replication.

**Production archetypes** (concurrency × size, with count as the sustained multiplier):

- **A. High concurrency + small events** (telemetry, order/event ingestion, many aggregates) → **`UNNEST` (adaptive)**. The batched journal's reason to exist.
- **B. Low concurrency + small events** (few hot aggregates, low-traffic service) → **`default`**; the `window` latency isn't offset by coalescing.
- **C. High concurrency + large events** (inline documents/snapshots) → **`add()` or `default`** (adaptive falls back automatically); better yet, **externalize the blob** and return to archetype A.
- **D. Bursty concurrency** (spikes, fan-out) → **batched**, for *stability* — the bounded queue + window absorbs bursts that would otherwise hammer the pool.
- **E. Busy singleton aggregate** → **`default`**; one entity can't self-coalesce.

**Decision matrix:**

| | small events (≲ tens of KB) | large events (≳ 64 KB) |
|---|---|---|
| low concurrency | default | default |
| high concurrency | **batched — `UNNEST` adaptive** | `add()` or default (externalize blobs) |
| bursty | **batched** (smooths load) | `add()` (bounded) |

**Bottom line:** production load breaks down along **round-trips + commit/fsync + connection-pool pressure**, and batching's whole value is collapsing those — which pays off most exactly where real systems live: **many concurrent small-event writers on a networked DB.** Event **size** decides whether you can use `UNNEST`; **concurrency** decides whether batching helps at all; **count** scales the WAL/disk/replication volume underneath.

---

## 14. Lessons learned

- **Measure the boring thing first.** The `max-requests` regime (window-bound vs size-flush) explained a "mysterious" discontinuity that no amount of SQL tuning would have.
- **A micro-optimization can be a macro-liability.** `UNNEST` looked like a clean win until a large payload turned a *slowdown* into a *crash*. Always probe the tails, including off-the-happy-path sizes.
- **Off-heap memory is invisible to `-Xmx`.** The OOM was direct-buffer memory; the fix was to bound the workload, not the heap.
- **The amicable fix is usually "prevent with one honest knob,"** not "catch and hope."
- **Loopback flatters the baseline.** The most important production number — the networked crossover — is the one a laptop can't give you.

---

## 15. Conclusion & future work

Adding an `UNNEST` insert to the batched journal is worthwhile — **conditionally**. For small events at high concurrency it is meaningfully faster than the existing `add()` batch (up to ~2–3× on loopback, likely more on a networked DB, and it lowers the concurrency break-even). For large events it is at best slower and at worst fatal, so it must be **bounded**. The **adaptive switch** — `UNNEST` under a size cap, `add()` above — captures the upside while making the downside unreachable, behind a single opt-in flag that stays off by default.

Just as valuable as the feature is the **map**: know your event size and your concurrency, and you know which of the three inserts (none, `add()`, `UNNEST`) you want.

**Future work:**

- **A binary `bytea[]` binding** (if the driver gains it) would remove the hex 2× and push the `UNNEST`-vs-`add()` crossover to larger events, shrinking (though not eliminating) the memory pressure.
- **Networked-DB benchmarks** (latency-injected or remote) to find the *real* production crossovers.
- **Auto-tuning `max-requests`** from observed concurrency, since it governs both the size-flush threshold and the memory bound.

---

## 16. Appendix: reproduce it yourself

Start a PostgreSQL and load the schema, then copy the benchmark harness into the test tree and run the three specs together:

```shell
# one sweep, all three strategies, directly comparable
sbt -Dpekko.bench.actorCounts=1,10,50,100,200,500,1000 \
    -Dpekko.bench.payloadSize=1024 \
    "core/Test/testOnly \
       org.apache.pekko.persistence.r2dbc.journal.BenchmarkDefaultManyActorsSpec \
       org.apache.pekko.persistence.r2dbc.journal.BenchmarkBatchedAddManyActorsSpec \
       org.apache.pekko.persistence.r2dbc.journal.BenchmarkBatchedUnnestManyActorsSpec"
```

- The `unnest` spec is the **adaptive** journal (`use-unnest = on`, default `8 KiB` cap).
- To reproduce the large-payload **OOM**, force raw uncapped `UNNEST`: add `-Dpekko.bench.unnestMaxPayloadSize=1GiB` and `-Dpekko.bench.payloadSize=262144`.

Raw result sets:

- [Concurrency & `max-requests` crossover](../benchmarks/journal-batched-write-throughput/results/2026-07-02-loopback-postgres-unnest-vs-add.txt)
- [Payload-size three-way (incl. 32/64 KB crossover)](../benchmarks/journal-batched-write-throughput/results/2026-07-02-loopback-postgres-unnest-payload-size.txt)
- [Large-payload stability / OOM](../benchmarks/journal-batched-write-throughput/results/2026-07-02-loopback-postgres-unnest-large-payload.txt)
- [Adaptive demonstration](../benchmarks/journal-batched-write-throughput/results/2026-07-03-loopback-postgres-adaptive-unnest.txt)

*All figures are loopback micro-benchmarks — directional, not authoritative. Measure your own workload against your own database.*

