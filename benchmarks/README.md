# Benchmarks

Reproducible performance benchmarks for this plugin, kept here to demonstrate capability and to allow results to be
re-measured over time. These are **not** part of the production build or the CI test suite - each benchmark harness is
stored here as a template and is copied into the test source tree only when you run it.

## Layout

Each benchmark lives in its own directory:

```
benchmarks/
  <benchmark-name>/
    README.md            # what it measures + how to replicate
    *.scala              # the harness (template, copied into core/src/test to run)
    results/             # raw result files, one per run (dated, with environment)
```

## Available benchmarks

| Benchmark | Measures |
|---|---|
| [`journal-batched-write-throughput`](journal-batched-write-throughput/README.md) | Write throughput of `StreamBatchedWriteJournal` vs the default `R2dbcJournal` as concurrency increases |

## General prerequisites

- JDK 17 (CI standard for this project).
- Docker, for a local database. PostgreSQL:

  ```shell
  docker compose -f docker/docker-compose-postgres.yml up -d
  docker exec -i docker-postgres-db-1 psql -U postgres -t < ddl-scripts/create_tables_postgres.sql
  ```

See each benchmark's `README.md` for the exact steps, parameters, and recorded results.

## Conventions

- Harness `.scala` files are templates: copy into `core/src/test/scala/...` to run, then remove afterwards so they do
  not become part of the compiled test suite. `sbt headerCreateAll` adds the license header once a file is in the
  source tree.
- Record each run under `results/` with a dated filename and the full environment (database version, JDK, Scala, host,
  pool size, and any tuned settings) so numbers are interpretable later.
- Numbers measured against a local loopback database are a lower bound for batching benefits; a networked database
  generally favours batching more (see the journal-batched-write-throughput notes).
