# Changelog

## [0.240.0](https://github.com/vgv/kolbasa/compare/v0.239.0...v0.240.0) - 2026-11-22

The first version of this changelog, nothing that changes kolbasa ([#159](https://github.com/vgv/kolbasa/pull/159)).

## [0.239.0](https://github.com/vgv/kolbasa/compare/v0.238.0...v0.239.0) - 2026-11-21

Shard map revisions and a safer shard map lookup ([#177](https://github.com/vgv/kolbasa/pull/177)).

### Schema

- The cluster shard map, `q__shard`, gets a `revision` column and a `q__shard_set_revision` trigger that sets it on
  every change. The next topology update adds both to an existing table, and its rows get the revision
  `1970-01-01 00:00:00+00`. Nothing is needed from you.

### Changed

- `butcher`: when `q__shard` on a node lacks some shards or can't be read, the error says so and names the node.
  Before, both said `Initialized shard table q__shard not found`.

### Fixed

- A shard map that couldn't be read on a node, for example because of a network error, was taken for a missing one.
  If no other node had a complete map, the cluster built a new one with random nodes on the node with the smallest
  id, and messages went to other nodes than before. Now the update fails with the node's name, and the old topology
  stays.
- A shard map with missing rows was ignored: the cluster built a new map on the node with the smallest id, with random
  nodes for every shard that wasn't there. Now the missing shards are added to the same map, and the other shards
  keep their nodes.

## [0.238.0](https://github.com/vgv/kolbasa/compare/v0.237.0...v0.238.0) - 2026-11-21

Library updates for the build and `butcher`, nothing that changes kolbasa
([#176](https://github.com/vgv/kolbasa/pull/176)).

## [0.237.0](https://github.com/vgv/kolbasa/compare/v0.236.0...v0.237.0) - 2026-11-21

The trace context moves to the `trace_context` column ([#175](https://github.com/vgv/kolbasa/pull/175)).

### Breaking changes

- Queue tables need the `trace_context` column now: every send and receive uses it. `createOrUpdateQueues` has added
  it since 0.227.0. If you manage the queue tables yourself, add `trace_context varchar[]` before you upgrade.

### Schema

- The trace context of a message is written to and read from `trace_context`. The old `opentelemetry` column is no
  longer written or read: it stays for older releases and will be dropped in a later one. While old and new releases
  run side by side, a message sent by one and received by the other has no trace context, so its spans aren't linked.

### Fixed

- A send with tracing on failed when a value of the trace context was longer than 1024 characters. Now there's no
  limit.

## [0.236.0](https://github.com/vgv/kolbasa/compare/v0.235.0...v0.236.0) - 2026-11-21

Highlights ([#174](https://github.com/vgv/kolbasa/pull/174)):

- Failed schema statements in a cluster are reported and retried
- `TopologyUpdateError` split into `UpdateFailed` and `SchemaFailed`

### Breaking changes

- `TopologyUpdateError` is now a sealed class of two kinds: `UpdateFailed`, with the `cause` as before, and the new
  `SchemaFailed`. A `TopologyUpdateErrorHandler` that reads `error.cause` has to check for `UpdateFailed` first.

### Added

- `Cluster.updateTopology()` returns a `ClusterSchemaResult`: what the schema did on every node.
- The Spring Boot starter retries the schema at startup: `kolbasa.standalone.schema.startup-attempts` and
  `kolbasa.cluster.schema.startup-attempts` times, 5 by default, with `startup-retry-delay` between tries, 1 second by
  default. If the last try fails, the application doesn't start. Before, the standalone schema got two tries with no
  pause.

### Fixed

- In a cluster, a schema statement that failed on a node was ignored: nothing was logged, and later updates didn't try
  it again, so the node stayed without the queue tables. Now the failure goes to `TopologyUpdateErrorHandler` as
  `SchemaFailed`, which logs a warning by default, and every next update tries again until all statements go through.
- The Spring Boot starter started an application with `KolbasaClusterSettings` even when the queue tables couldn't be
  created on some nodes. Now the application doesn't start, and the error lists the failed statements with their
  nodes.

## [0.235.0](https://github.com/vgv/kolbasa/compare/v0.234.0...v0.235.0) - 2026-11-21

Spring Boot 4 starter ([#169](https://github.com/vgv/kolbasa/pull/169)).

### Added

- Spring Boot 4 starter, `io.github.vgv:kolbasa-spring-boot-4-starter`, for Spring Boot 4.0.0 or newer. Declare a
  `KolbasaStandaloneSettings` bean for queues in the application's own database, or a `KolbasaClusterSettings` bean
  for a cluster. The starter then creates the schema and the `Producer`, `Consumer`, `Mutator` and `Inspector` beans,
  and starts and stops your `QueueWorker` beans.
- New docs page: [Spring Boot](<docs/Spring Boot.md>).
- `Queue.withName()` and `Queue.withOptions()`: a copy of a queue with another name or other options.

### Fixed

- With Gradle, an application on Kotlin 2.1 couldn't compile with kolbasa. The POMs declared `kotlin-stdlib` 2.3.21,
  the version of our compiler, Gradle raised the application's standard library to it, and a Kotlin 2.1 compiler
  can't read it. Now `kolbasa` and `kolbasa-opentelemetry` declare `kotlin-stdlib` 2.1.0, the oldest Kotlin they
  support.

## [0.234.0](https://github.com/vgv/kolbasa/compare/v0.233.0...v0.234.0) - 2026-11-21

`kolbasa-opentelemetry` works with OpenTelemetry 1.12 or newer ([#168](https://github.com/vgv/kolbasa/pull/168)).

### Changed

- `kolbasa-opentelemetry` no longer depends on the OpenTelemetry instrumentation libraries,
  `opentelemetry-instrumentation-api` and `opentelemetry-instrumentation-api-incubator`. Its only dependency now is
  the core `opentelemetry-api`, 1.12.0 or newer, so it works with the OpenTelemetry version your application already
  has. Before, it required `opentelemetry-api` 1.66.0. The spans keep their names.

## [0.233.0](https://github.com/vgv/kolbasa/compare/v0.232.0...v0.233.0) - 2026-11-21

Async calls take any `Executor` ([#167](https://github.com/vgv/kolbasa/pull/167)).

### Breaking changes

- `Kolbasa.asyncExecutor` and `asyncExecutor` in `ProducerOptions`, `SendOptions` and `MutatorOptions` (and their
  builders) are an `Executor` now, not an `ExecutorService`. Setting them works as before, and any `Executor` fits,
  Spring's `TaskExecutor` too. Code that reads one as an `ExecutorService`, for example to shut down
  `Kolbasa.asyncExecutor`, has to keep its own reference instead.

### Fixed

- The default `Kolbasa.asyncExecutor` thread lived as long as the JVM and kept what it took from the thread that
  started it, such as the context classloader. Under Spring Boot devtools, that kept the application from before a
  restart in memory. Now the thread ends after a minute without calls, and the next call starts a new one.

## [0.232.0](https://github.com/vgv/kolbasa/compare/v0.231.0...v0.232.0) - 2026-11-15

Metrics go through Micrometer instead of the Prometheus client ([#160](https://github.com/vgv/kolbasa/pull/160)).

### Breaking changes

- `PrometheusConfig` (package `kolbasa.telemetry.prometheus`) is replaced by `MetricsConfig` (package
  `kolbasa.telemetry.metrics`). `MetricsConfig.Config` takes a Micrometer `MeterRegistry` and has no default. Metrics
  need Micrometer 1.13.15 or newer: kolbasa doesn't bring it. To keep Prometheus, add
  `io.micrometer:micrometer-registry-prometheus` and pass its registry:
  ```kotlin
  val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
  Kolbasa.metricsConfig = MetricsConfig.Config(registry)
  ```
- `Kolbasa.prometheusConfig` is renamed to `metricsConfig`, and so is `prometheusConfig` in `ProducerOptions`,
  `ConsumerOptions`, `MutatorOptions` and their builders. `SweepHelper.sweep(connection, queue, limit,
  prometheusConfig)` takes a `MetricsConfig` now.
- In Prometheus, `kolbasa_queue_size` is now `kolbasa_queue_size_bytes`. Update your dashboards and alerts. The other
  names stay the same, and every duration gets one more series, `_max`.

### Added

- New docs page: [Metrics](docs/Metrics.md). It replaces `Prometheus metrics.md`.

### Fixed

- A producer or consumer that was no longer used stayed in a static SQL cache of up to 1000 entries, with its
  options and the metrics registry in them. Now each producer and consumer has its own cache, which goes away with it.

## [0.231.0](https://github.com/vgv/kolbasa/compare/v0.230.0...v0.231.0) - 2026-11-08

Highlights ([#158](https://github.com/vgv/kolbasa/pull/158)):

- The shard strategy is set per `ClusterProducer`
- `ProducerOptions.shard` removed

### Breaking changes

- `Kolbasa.shardStrategy` is removed. Pass the strategy to the `ClusterProducer` constructor instead:
  ```kotlin
  val producer = ClusterProducer(cluster, shardStrategy = ShardStrategy.Random)
  ```
  From Java, pass the options too:
  ```java
  var producer = new ClusterProducer(cluster, ProducerOptions.DEFAULT, ShardStrategy.Random.INSTANCE);
  ```
  The default is still `ShardStrategy.ThreadLocalWithInterval()`.
- `ProducerOptions.shard` and `ProducerOptions.Builder.shard()` are removed. Set `SendOptions.shard` for one call, or
  give the producer `ShardStrategy.Fixed(shard)` to send all its messages to one shard.

## [0.230.0](https://github.com/vgv/kolbasa/compare/v0.229.0...v0.230.0) - 2026-11-08

Highlights ([#156](https://github.com/vgv/kolbasa/pull/156)):

- `ClusterTopologyUpdater` replaces `Cluster.initAndScheduleStateUpdate()`
- Queues can be added to a `Cluster` without a restart

### Breaking changes

- `Cluster.initAndScheduleStateUpdate()` is removed. Start a `ClusterTopologyUpdater` instead:
  ```kotlin
  val updater = ClusterTopologyUpdater(cluster)
  updater.start()
  ```
- `Kolbasa.clusterStateUpdateConfig` and `ClusterStateUpdateConfig` are removed. Set the interval and the executor in
  `ClusterTopologyUpdaterOptions`.
- `Cluster.updateStateOnce()` is renamed to `updateTopology()`, and `clusterStateUpdatedAtLeastOnce()` to
  `isInitialized()`.
- The `Cluster` constructors need the queues: `Cluster(dataSources, queues)` or
  `Cluster(dataSourcesProvider, queuesProvider)`. Pass an empty list if you create the queue tables yourself. With
  changing nodes and fixed queues, write `Cluster({ currentNodes() }, { listOf(orders) })`.

### Added

- `ClusterTopologyUpdater`, with `ClusterTopologyUpdaterOptions`: keeps the topology of a `Cluster` fresh on a
  schedule, until `close()`.
- The queues of a `Cluster` can come from a `Supplier`. A queue added to it is created on every node by the next
  topology update, or at once by `updateTopology()`.

### Fixed

- A node added to a running cluster didn't get the queue tables: they were created only at the first update of a
  `Cluster`. Now a new node or queue gets them on the next topology update.
- A scheduled update that failed, for example while a node was down, stopped all later updates for good, and nothing
  was logged. Now the cluster keeps its last topology, the error goes to `TopologyUpdateErrorHandler`, which logs a
  warning through `System.Logger` by default, and the next update runs on time.

## [0.229.0](https://github.com/vgv/kolbasa/compare/v0.228.0...v0.229.0) - 2026-11-07

Highlights ([#155](https://github.com/vgv/kolbasa/pull/155)):

- Prometheus metrics set per producer, consumer and mutator
- `PrometheusConfig.Config.preciseStringSize` removed

### Breaking changes

- `PrometheusConfig.Config.preciseStringSize` is removed. Drop the argument.

### Added

- `prometheusConfig` in `ProducerOptions`, `ConsumerOptions` and `MutatorOptions`: metrics for one producer, consumer
  or mutator instead of the global `Kolbasa.prometheusConfig`. `PrometheusConfig.None` switches them off for that one.
- `SweepHelper.sweep(connection, queue, limit, prometheusConfig)`: a sweep with its own metrics.
- New docs pages: [Prometheus metrics](<docs/Prometheus metrics.md>) and [SQL dump](<docs/SQL dump.md>).

### Changed

- `Kolbasa.prometheusConfig` is read on every call and every sweep, so a change takes effect on the next call.
  Before, it had to be set before any other use of kolbasa, and later changes did nothing.
- Several `PrometheusConfig.Config` objects over one registry share the same metrics. Before, the second one failed
  with `duplicate metric name`.
- `kolbasa_producer_send_bytes` and `kolbasa_consumer_receive_bytes` count strings in UTF-8 bytes, as
  `preciseStringSize = true` did. Before, the default counted characters, so with non-ASCII data the values grow.
- SQL dump records show the queue and, in a cluster, the node. Durations have microseconds, for example `0.412ms`.
- The SQL dump prints the retention cleanup of a DLQ or archive under the main queue, as a step of its sweep.
  Register the main queue to see it.

### Fixed

- `SqlDumpConfig.Builder.queue(queue)` without any `StatementKind` threw. Now it prints every statement of the queue.
- `SqlDumpConfig.Builder.build()` shared its map with the built config, so later builder calls changed a config that
  was already built. Now they don't.
- `Rows:` in a `PRODUCER_INSERT` dump record counted skipped duplicates too. Now it counts only the inserted rows.

## [0.228.0](https://github.com/vgv/kolbasa/compare/v0.227.0...v0.228.0) - 2026-11-01

PostgreSQL JDBC driver 42.7.14 ([#154](https://github.com/vgv/kolbasa/pull/154)).

### Changed

- `kolbasa` depends on the PostgreSQL JDBC driver 42.7.14, up from 42.7.13.

## [0.227.0](https://github.com/vgv/kolbasa/compare/v0.226.0...v0.227.0) - 2026-11-01

Highlights ([#147](https://github.com/vgv/kolbasa/pull/147)):

- `QueuePipe` factories take `PipeSource` and `PipeTarget`
- `Filter.nativeSql` patterns are no longer `MessageFormat` patterns

### Breaking changes

- `QueuePipe.asIs`, `map`, `flatMap` and `mapBatch` take a source and a target instead of four arguments: write
  `PipeSource(from, consumer)` and `PipeTarget(to, producer)` in place of `from, consumer, to, producer`.
- `Filter.nativeSql` no longer reads the pattern as a `java.text.MessageFormat` pattern. If you doubled single quotes
  (`''`) to get one, write one now.

### Schema

- `createOrUpdateQueues` adds a `trace_context` column to existing queue tables, and new tables get it too. It is the
  column dropped in 0.226.0, now without a length limit. Nothing reads or writes it yet, and adding it doesn't
  rewrite the table.

### Fixed

- With `PartialInsert.PROHIBITED`, when a chunk failed outside PostgreSQL, for example in the serializer, the chunks
  before it were saved anyway, though `SendResult` said all messages failed. Now nothing is saved.
- With `PartialInsert.PROHIBITED`, a send that PostgreSQL rejected in `ConnectionAwareDatabaseProducer` aborted your
  whole transaction, so every later statement failed. Now the failed messages are rolled back to a savepoint, and
  your transaction stays usable.
- A `Filter.nativeSql` pattern with a string literal, for example `{0}::text like '1%'`, lost its quotes and failed
  in PostgreSQL. Now only `{0}`, `{1}` and so on are replaced, and the rest goes into SQL as written.
- A filter like `EVENT_TIME eq x` on an `InstantField` could miss the message sent with the same `x`, if `x` had
  nanoseconds: the sent value was cut to microseconds, the filter value was rounded. Now both are cut.
- A mutation with a negative duration longer than about 24.8 days, for example
  `AddScheduledAt(Duration.ofDays(-30))`, failed on PostgreSQL 10–14 with "interval field value out of range". Now
  it works.

## [0.226.0](https://github.com/vgv/kolbasa/compare/v0.225.0...v0.226.0) - 2026-10-31

Highlights ([#146](https://github.com/vgv/kolbasa/pull/146)):

- Queue pipes
- `QueueProcessor` moved to `kolbasa.worker.processor`

### Breaking changes

- `QueueProcessor`, `QueueProcessorOptions`, `MessageHandler`, `HandleResult`, `ProcessError` and
  `ProcessErrorHandler` moved from `kolbasa.worker` to `kolbasa.worker.processor`. Change the imports. The logger of
  `ProcessErrorHandler.SYSTEM_LOG` is now `kolbasa.worker.processor.ProcessErrorHandler`, so update your log
  configuration if it names the old one.
- The `QueueProcessor` constructor takes the queue first: `QueueProcessor(queue, consumer, options, handler)` instead
  of `QueueProcessor(consumer, queue, options, handler)`.

### Schema

- `createOrUpdateQueues` drops the `trace_context` column from existing queue tables, and new tables don't get it.
  The column was too short for some trace contexts. Nothing read or wrote it, so no data is lost, and the drop
  doesn't rewrite the table.

### Added

- `QueuePipe` (package `kolbasa.worker.pipe`): moves messages from one queue to another, usually on another server,
  one batch per `process()` call. Make it with `QueuePipe.asIs`, `map`, `flatMap` or `mapBatch`. Transform errors go
  to `TransformErrorHandler`, which logs through `System.Logger` by default.
- `BatchProcessor`: what a `QueueWorker` runs. `QueueProcessor` and `QueuePipe` implement it, and so can your own
  class.
- `SendRequest(data)` without options from Java: the constructor is now `@JvmOverloads`.

## [0.225.0](https://github.com/vgv/kolbasa/compare/v0.224.0...v0.225.0) - 2026-10-31

Highlights ([#145](https://github.com/vgv/kolbasa/pull/145)):

- `Producer`, `Consumer` and `Mutator` now expose the options they were created with
- `QueueProcessorOptions.tracing` removed: set tracing on the consumer

### Breaking changes

- `QueueProcessorOptions.tracing` and `QueueProcessorOptions.Builder.tracing()` are removed. A `QueueProcessor` now
  uses the tracing of its consumer: `ConsumerOptions.tracing`, or `Kolbasa.tracing` if the consumer has none. Set the
  tracing on the consumer instead. If the processor gets your own `Consumer` that wraps another one, override
  `options` in it to return the wrapped consumer's options, or the processor falls back to `Kolbasa.tracing`.

### Added

- `options` on `Producer`, `Consumer`, `Mutator` and their `ConnectionAware*` twins: the options the role was created
  with. Your own implementations compile unchanged: by default it returns `ProducerOptions.DEFAULT`,
  `ConsumerOptions.DEFAULT` or `MutatorOptions.DEFAULT`.

## [0.224.0](https://github.com/vgv/kolbasa/compare/v0.223.0...v0.224.0) - 2026-10-31

`QueueProcessor` and `QueueWorker`, a ready poll loop ([#144](https://github.com/vgv/kolbasa/pull/144)).

### Added

- `QueueProcessor` and `QueueWorker` (package `kolbasa.worker`), a ready poll loop. `QueueProcessor.process()` handles
  one batch: it receives the messages, passes each one to your `MessageHandler` and deletes the handled ones.
  `QueueWorker` runs it in its own threads, with pauses and a growing back-off on errors. Errors go to
  `ProcessErrorHandler` and `WorkerErrorHandler`, which log through `System.Logger` by default.
- `Tracing.process(call)`: a hook around every `MessageHandler` call.
- `OpenTelemetryTracing` makes a span for every handled message. Its parent is the span that sent the message, so
  what the handler does lands in the sender's trace.

### Changed

- `Tracing.send()` and `Tracing.receive()` now have default implementations that just proceed, like `process()`. A
  hook overrides only what it needs.
- `OpenTelemetryTracing`: a `receive()` that finds nothing makes no span when no span is current, as in a
  `QueueWorker` thread polling an empty queue. Inside a span of yours it still makes one. A failed `receive()` always
  makes one.
- `OpenTelemetryTracing`: the span of a `send()` with one message has `messaging.message.id`, if the message was
  sent.

## [0.223.0](https://github.com/vgv/kolbasa/compare/v0.222.0...v0.223.0) - 2026-10-25

Unreadable messages no longer block the queue ([#143](https://github.com/vgv/kolbasa/pull/143)).

### Added

- `ConsumerOptions.readErrorHandler`, with `ReadErrorHandler` and `ReadError`: what to do with a received message
  that can't be read. A handler must not throw, or `receive()` fails as it did before.
- Prometheus counter `kolbasa_consumer_receive_messages_unreadable` (labels `queue`, `node_id`).

### Fixed

- A received message whose data or meta fields couldn't be read made the whole `receive()` throw. Its transaction
  rolled back, so the message didn't spend an attempt and came back on the next call: one bad message blocked the
  queue. Now `receive()` skips it and returns the rest of the batch. By default, a warning goes to `System.Logger`
  (`kolbasa.consumer.ReadErrorHandler`), or an error on the last attempt. The skipped message spends its attempt as
  usual, and when its attempts run out it goes to the DLQ, or is deleted if the queue has no DLQ.

## [0.222.0](https://github.com/vgv/kolbasa/compare/v0.221.0...v0.222.0) - 2026-10-24

Highlights ([#142](https://github.com/vgv/kolbasa/pull/142)):

- `OpenTelemetryTracing` moved to its own artifact, `kolbasa-opentelemetry`
- `kolbasa.stats` renamed to `kolbasa.telemetry`

### Breaking changes

- `OpenTelemetryTracing` moved out of `kolbasa` into a new artifact, `io.github.vgv:kolbasa-opentelemetry`, and from
  package `kolbasa.stats.opentelemetry` to `kolbasa.opentelemetry`. Add the dependency and change the import.
- Package `kolbasa.stats` is renamed to `kolbasa.telemetry`. Class names stay the same, only the imports change:
  `kolbasa.stats.prometheus` → `kolbasa.telemetry.prometheus`, `kolbasa.stats.sql` → `kolbasa.telemetry.sql`,
  `kolbasa.stats.tracing` → `kolbasa.telemetry.tracing`.

### Added

- `Tracing.wrapAsync(task)`: lets a hook carry its context into the thread where `sendAsync()` runs. By default it
  returns `task` as is, so existing `Tracing` implementations compile unchanged.

### Changed

- `kolbasa-opentelemetry` brings `opentelemetry-api`, `opentelemetry-instrumentation-api` and
  `opentelemetry-instrumentation-api-incubator` as its own dependencies. In `kolbasa` they were compile-only, so you
  had to add them yourself.

### Fixed

- With `OpenTelemetryTracing`, the span of a `sendAsync()` had no parent and started a new trace, unless the executor
  carried the trace context itself. Now the caller's current span is always its parent.
