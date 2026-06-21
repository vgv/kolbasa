# AGENTS.md

Guidance for AI coding agents working in this repository.

## What this is

**kolbasa** is a reliable message & job queue for Java & Kotlin, built on PostgreSQL — no Kafka, no
RabbitMQ, just the database you already run. It is a pure Kotlin library (usable from any JVM language),
published to Maven Central as `io.github.vgv:kolbasa`. Requires PostgreSQL 10+ and JVM 17+.

## Build, test, run

This is a **Gradle** project (Kotlin DSL). Use the wrapper:

- Build: `./gradlew build`
- Compile only: `./gradlew compileKotlin`
- Run tests: `./gradlew test`
- Run a single example: `./gradlew example -P name=SimpleExample` (any file from
  `src/test/kotlin/examples`)

**Tests need Docker.** The test suite and the examples spin up PostgreSQL via Testcontainers, so a
running Docker daemon is required. Examples can also point at a real PostgreSQL instance via
`src/test/kotlin/examples/ExamplesDataSourceProvider.kt`.

Toolchain: JVM 17, Kotlin API/language level 1.9, JUnit 5.

## Layout

- `src/main/kotlin/kolbasa/` — the library. Key packages: `producer`, `consumer`, `mutator`,
  `inspector` (the four roles), `queue` (queue/options/meta-fields), `schema` (DDL generation),
  `cluster` (multi-node, incl. `cluster/butcher` — the operator CLI), `stats` (Prometheus,
  OpenTelemetry).
- `src/test/kotlin/examples/` — runnable, self-contained examples; the best on-ramp to the API.
- `docs/` — architecture and operator documentation (see below).

## Conventions

- Follow the style of the surrounding code; match its naming, structure, and KDoc density. Public APIs
  carry KDoc.
- Both a `DataSource`-backed (`Database*`) and a `Connection`-aware (`ConnectionAware*`) variant exist
  for each role — keep them in sync when changing one.
- kolbasa runs on **vanilla PostgreSQL** (no extensions, no superuser). Don't introduce SQL that needs
  either.
- Don't hand-edit a queue's generated DDL; schema generation owns table/index structure.
- Run `./gradlew test` after changes. Don't commit, stage, or push unless explicitly asked.

## Documentation

These explain how kolbasa works under the hood — read the relevant one before changing the area it
covers:

- [docs/Architecture.md](docs/Architecture.md) — single-node model: the queue model, meta-fields, the
  anatomy of a queue table, the message lifecycle, deduplication, DLQ and archive, sweep, schema
  generation.
- [docs/Patterns.md](docs/Patterns.md) — recipes built from the primitives: priority queue,
  time-to-live (TTL), at-most-once delivery.
- [docs/Cluster architecture.md](<docs/Cluster architecture.md>) — multi-node: shards, routing, the
  shard-ownership map, and shard migration.
- [docs/Butcher.md](docs/Butcher.md) — operator guide for `butcher`, the cluster health-check and
  rebalancing CLI.
- [docs/Message IDs.md](<docs/Message IDs.md>) — how the 64-bit `id` is structured (per-node bucket) so
  ids stay globally unique across nodes.
- [docs/Message state transitions.md](<docs/Message state transitions.md>) — the message lifecycle
  deep-dive (states, transitions, uniqueness scopes).

The [README](README.md) has a feature overview and walkthroughs of the examples.
