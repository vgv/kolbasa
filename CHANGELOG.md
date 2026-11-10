# Changelog

All notable changes to kolbasa (`io.github.vgv:kolbasa`). Versions are newest first.

## [0.231.0] – 2026-11-08

Shard strategy improvements ([#158](https://github.com/vgv/kolbasa/pull/158)).

### Breaking changes

- **`Kolbasa.shardStrategy` is removed.** The shard strategy is now set per producer, in the new third constructor
  parameter of `ClusterProducer`:
  ```kotlin
  val producer = ClusterProducer(cluster, shardStrategy = ShardStrategy.Random)
  ```
  ```java
  var producer = new ClusterProducer(cluster, ProducerOptions.DEFAULT, ShardStrategy.Random.INSTANCE);
  ```
  The default is still `ShardStrategy.ThreadLocalWithInterval()`.
- **`ProducerOptions.shard` (and `ProducerOptions.Builder.shard()`) is removed.** A message's shard now comes from
  `SendOptions.shard` or, when that is not set, from the producer's `ShardStrategy`. To send everything of a producer
  to one shard, pass `ShardStrategy.Fixed`.

### Documentation

- `SendOptions.shard`, `SendRequest`, `ShardStrategy` and the architecture docs describe the new two-step shard
  resolution.

## [0.230.0] – 2026-11-08

Cluster rework ([#156](https://github.com/vgv/kolbasa/pull/156)).

### Breaking changes

- **The topology refresh moved out of `Cluster` into the new `ClusterTopologyUpdater`.**
  `Cluster.initAndScheduleStateUpdate()` is removed; start an updater instead:
  ```kotlin
  val updater = ClusterTopologyUpdater(cluster)
  updater.start()
  ```
- **`Kolbasa.clusterStateUpdateConfig` and `ClusterStateUpdateConfig` are removed.** The interval, executor and error
  handler are set in `ClusterTopologyUpdaterOptions` (also via `ClusterTopologyUpdaterOptions.builder()`).
- Renames in `Cluster`: `updateStateOnce()` → `updateTopology()`, `clusterStateUpdatedAtLeastOnce()` →
  `isInitialized()`. The internal `ClusterState` is now `ClusterTopology`.
- The `Cluster` constructors take queues explicitly: `Cluster(dataSources, queues)` or
  `Cluster(dataSourcesProvider, queuesProvider)`. The `@JvmOverloads` defaults with an empty queue list are gone —
  pass `emptyList()` to keep managing the schema yourself.
- The error for an uninitialized cluster now reads `Cluster topology isn't initialized, call updateTopology() or
  ClusterTopologyUpdater.start() first`.

### Added

- `ClusterTopologyUpdater` — schedules topology updates, is `AutoCloseable` and can be stopped. The first update in
  `start()` throws to the caller; later failures go to the error handler, and the cluster keeps its previous topology.
- `ClusterTopologyUpdaterOptions` — `interval` (default 1 minute, allowed range 1 second … 15 minutes), `executor`,
  `errorHandler`.
- `TopologyUpdateErrorHandler` and `TopologyUpdateError` — a hook for failed scheduled updates. The default,
  `TopologyUpdateErrorHandler.SYSTEM_LOG`, logs a warning through `System.Logger`.
- **Dynamic queues:** the queue list of a `Cluster` can be a `Supplier`. A queue added to it is created on every node
  by the next topology update (or immediately, by calling `updateTopology()`).

### Changed

- Schema generation in a cluster is cached by the set of nodes and queues: it reruns whenever a node or a queue
  appears, not only once per `Cluster` instance. If a node fails, the next update retries.

## [0.229.0] – 2026-11-07

Prometheus and SQL dump improvements ([#155](https://github.com/vgv/kolbasa/pull/155)).

### Added

- Per-role Prometheus configuration: `ProducerOptions.prometheusConfig`, `ConsumerOptions.prometheusConfig`,
  `MutatorOptions.prometheusConfig` (plus builder methods). `PrometheusConfig.None` switches metrics off for one role;
  `null` falls back to `Kolbasa.prometheusConfig`.
- `SweepHelper.sweep(connection, queue, limit, prometheusConfig)` — a sweep with its own metrics config.
- SQL dump records now include the queue name and, in a cluster, the node id. Durations are printed with microsecond
  precision (`0.412ms`).
- New docs: [Prometheus metrics](<docs/Prometheus metrics.md>) and [SQL dump](<docs/SQL dump.md>).

### Changed

- `Kolbasa.prometheusConfig` is read on every producer/consumer/mutator call and every sweep, so a change takes
  effect immediately. Previously it had to be set before any other use of kolbasa and was then fixed for the life of
  the JVM.
- Metrics are no longer cached inside the `Queue` object.
- Retention cleanup of DLQ/archive tables is dumped under the main queue (the cleanup is a step of its sweep).

### Fixed

- `SqlDumpConfig.Builder.queue(queue)` without any `StatementKind` threw; it now enables every statement kind for
  the queue.
- `SqlDumpConfig.Builder.build()` shared its mutable map with the built config; later builder calls no longer change
  an already built config.

## [0.228.0] – 2026-11-01

Dependency upgrades ([#154](https://github.com/vgv/kolbasa/pull/154)).

### Changed

- Prometheus client 1.9.0 → 1.10.0.
- PostgreSQL JDBC driver 42.7.13 → 42.7.14.
- Logback 1.6.5 → 1.6.6 (examples/tests).
- Gradle 9.8.0 → 9.8.2; `actions/setup-java` 6.0.1 → 6.1.0 in CI.

## [0.227.0] – 2026-11-01

Fixes and improvements ([#147](https://github.com/vgv/kolbasa/pull/147)).

### Breaking changes

- **`QueuePipe` factories take `PipeSource` / `PipeTarget` pairs** instead of four separate arguments:
  ```kotlin
  val pipe = QueuePipe.map(PipeSource(orders, consumer), PipeTarget(remoteOrders, producer)) { message ->
      SendMessage(convert(message.data))
  }
  ```

### Schema

- Queue tables get a new `trace_context varchar[]` column without a length limit (added to existing tables on the
  next `createOrUpdateQueues`). Nothing reads or writes it yet; it is a step in renaming the `opentelemetry` column.
  Schema updates run one `ALTER TABLE` per transaction in this release, so locks are held on one table at a time.

### Fixed

- **`PartialInsert.PROHIBITED` in `ConnectionAwareDatabaseProducer`:** an error outside PostgreSQL (e.g. in the
  serializer) could leave earlier chunks inserted. The whole request now runs under a savepoint — all chunks or none,
  and the caller's transaction stays usable.
- `Filter.nativeSql` used `MessageFormat`, which ate single quotes and braces. Only `{0}`, `{1}`, … are now replaced;
  everything else goes into SQL as written. A placeholder without a matching field throws `IllegalArgumentException`.
- `InstantField`: filter values were rounded by the driver while sent values were truncated to microseconds, so
  `eq` could miss a message sent with the same `Instant`. Both are now truncated the same way.
- `AddScheduledAt` with a large negative `delta` failed on PostgreSQL 10–14: the interval unit was picked by the
  signed value, so it was always written in milliseconds and overflowed `int`. The unit is now picked by the absolute
  value, so negative deltas of any size work (messages become visible earlier).

### Documentation

- README, Architecture, Patterns and the pipe/transaction examples are updated.

## [0.226.0] – 2026-10-31

Queue pipes ([#146](https://github.com/vgv/kolbasa/pull/146)).

### Added

- **`QueuePipe`** (`kolbasa.worker.pipe`) — moves messages from one queue to another, usually on another server. Works
  with any `Consumer`/`Producer`, cluster ones included. Factories:
  - `asIs` — moves the stored data and metadata without your serializer/deserializer (both queues must use the same
    data type kind);
  - `map` / `flatMap` — transform each message (`MapTransform`, `FlatMapTransform`);
  - `mapBatch` — turns a whole batch into one `SendRequest` (`BatchTransform`), so you choose its shard and send
    options.
- `QueuePipeOptions` — `receiveLimit`, `receiveOptions` (metadata is read by default), `errorHandler`.
- `TransformErrorHandler` / `TransformError` — what to do when a transform fails; the default is `SYSTEM_LOG`.
- `BatchProcessor` — a common interface of `QueueProcessor` and `QueuePipe`; `QueueWorker` now runs any
  `BatchProcessor`.
- `SendRequest` constructor got `@JvmOverloads`.
- Docs: routing with pipes and more recipes in [Patterns](docs/Patterns.md); pipe examples.

### Breaking changes

- `QueueProcessor`, `QueueProcessorOptions`, `MessageHandler`, `HandleResult`, `ProcessError` and
  `ProcessErrorHandler` moved from `kolbasa.worker` to **`kolbasa.worker.processor`**.
- `QueueProcessor` constructor order changed: `QueueProcessor(queue, consumer, options, handler)`.

### Schema

- The `trace_context` column added in 0.221.0 is **dropped** from existing queue tables: it was created as
  `varchar(1024)[]`, too short for some trace contexts. Nothing read or wrote it, so no data is lost. It comes back
  without a length limit in 0.227.0. Schema updates run one `ALTER TABLE` per transaction.

### Fixed

- `QueuePipe.asIs` always reads the metadata, even when custom `receiveOptions` (e.g. with a filter) don't ask for it,
  so metadata is never silently lost.

## [0.225.0] – 2026-10-31

Options exposed on role interfaces ([#145](https://github.com/vgv/kolbasa/pull/145)).

### Added

- `Producer.options`, `Consumer.options`, `Mutator.options` (and their `ConnectionAware*` counterparts) — the options
  of a role are readable through the interface. The defaults are `*Options.DEFAULT`.

### Breaking changes

- `QueueProcessorOptions.tracing` (and its builder method) is removed. `QueueProcessor` now uses the tracing of its
  consumer (`ConsumerOptions.tracing`, or the global `Kolbasa.tracing`), so receive and handler spans always go to
  the same hook.

## [0.224.0] – 2026-10-31

Queue worker ([#144](https://github.com/vgv/kolbasa/pull/144)).

### Added

- **`QueueProcessor`** — handles one batch per `process()` call: receives up to `receiveLimit` messages, calls your
  `MessageHandler` for each one and deletes the handled ones. Fits any loop or scheduler. Returns a `ProcessResult`
  (`succeeded`, `skipped`, `failed`, `requested`, `received`, `isBatchFull`).
- `MessageHandler` and `HandleResult` (`DONE` — delete the message, `SKIP` — leave it to come back after the
  visibility timeout).
- `QueueProcessorOptions` — `receiveLimit`, `receiveOptions`, `errorHandler`; `ProcessErrorHandler` / `ProcessError`
  for handler failures (default `SYSTEM_LOG`).
- **`QueueWorker`** — runs a processor in its own threads: `start()`, `stop(timeout)`, `AutoCloseable`. Exponential
  back-off with jitter on errors.
- `QueueWorkerOptions` — `threads`, `maxBatchesBeforePause`, `successPause` (5 s), `errorPause` (5 s),
  `maxErrorPause` (1 min), `errorHandler`, `stopTimeout` (30 s), `threadFactory`. `WorkerErrorHandler` /
  `WorkerError` for loop failures.
- `Tracing.process(call: ProcessCall)` — a hook around every handler call. All `Tracing` methods now have default
  pass-through implementations, so a hook overrides only what it needs.
- `kolbasa-opentelemetry`:
  - a process span for every handler call, linked to the message's send span, with `kolbasa.process.result` and
    `kolbasa.message.last_attempt` attributes;
  - the message id on a send span of a single message;
  - an empty receive with no parent span no longer creates a span (a failed receive still does).
- `QueueWorkerExample` (Kotlin and Java).

### Documentation

- KDoc across the public API converted to the multi-line form; README and AGENTS.md updated.

## [0.223.0] – 2026-10-25

Poison message handling ([#143](https://github.com/vgv/kolbasa/pull/143)).

### Added

- `ConsumerOptions.readErrorHandler` and `ReadErrorHandler` / `ReadError` — what to do with a received message whose
  data or meta fields can't be read. Such a message is skipped, and `receive()` returns the rest of the batch. It
  spends its attempt as usual and ends up in the DLQ (or is deleted) when attempts run out. Built-in handlers:
  `SYSTEM_LOG` (default; a warning, or an error on the last attempt) and `SKIP`.
- Prometheus counter `kolbasa_consumer_receive_messages_unreadable` (labels `queue`, `node_id`).

### Fixed

- One message that failed to deserialize made the whole `receive()` fail. Its transaction rolled back, so the message
  didn't spend an attempt and came first again on the next call, blocking the queue.

## [0.222.0] – 2026-10-24

OpenTelemetry as a separate module ([#142](https://github.com/vgv/kolbasa/pull/142)).

### Breaking changes

- **OpenTelemetry support moved to a separate artifact, `io.github.vgv:kolbasa-opentelemetry`.** Add the dependency
  to keep using it. `OpenTelemetryTracing` moved from `kolbasa.stats.opentelemetry` to `kolbasa.opentelemetry`.
- Package `kolbasa.stats` is renamed to **`kolbasa.telemetry`**: `kolbasa.telemetry.prometheus`,
  `kolbasa.telemetry.sql`, `kolbasa.telemetry.tracing`. Update the imports.

### Added

- `Tracing.wrapAsync(task)` — called by every `sendAsync()` in the caller's thread, so a hook can carry the trace
  context into the executor thread. `OpenTelemetryTracing` uses it, so async send spans get the right parent.
- `OpenTelemetryExample` (Kotlin and Java) and a README section on tracing.

[0.231.0]: https://github.com/vgv/kolbasa/compare/v0.230.0...v0.231.0
[0.230.0]: https://github.com/vgv/kolbasa/compare/v0.229.0...v0.230.0
[0.229.0]: https://github.com/vgv/kolbasa/compare/v0.228.0...v0.229.0
[0.228.0]: https://github.com/vgv/kolbasa/compare/v0.227.0...v0.228.0
[0.227.0]: https://github.com/vgv/kolbasa/compare/v0.226.0...v0.227.0
[0.226.0]: https://github.com/vgv/kolbasa/compare/v0.225.0...v0.226.0
[0.225.0]: https://github.com/vgv/kolbasa/compare/v0.224.0...v0.225.0
[0.224.0]: https://github.com/vgv/kolbasa/compare/v0.223.0...v0.224.0
[0.223.0]: https://github.com/vgv/kolbasa/compare/v0.222.0...v0.223.0
[0.222.0]: https://github.com/vgv/kolbasa/compare/v0.221.0...v0.222.0
