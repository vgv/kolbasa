# Metrics

kolbasa can report what it does: how many messages your producers send, how many your consumers receive and delete,
how long each call takes, and how big and how old each queue is. It reports through
[Micrometer](https://micrometer.io/), so the metrics go wherever your application sends its own: Prometheus, OTLP,
Datadog, CloudWatch or any other backend that Micrometer supports.

Metrics are off by default. They cost nothing until you turn them on.

## Table of Contents

1. [Turning metrics on](#turning-metrics-on)
2. [Prometheus without Spring](#prometheus-without-spring)
3. [Per producer, consumer and mutator](#per-producer-consumer-and-mutator)
4. [Metric reference](#metric-reference)
5. [Tags](#tags)
6. [How queue state is updated](#how-queue-state-is-updated)
7. [Bytes](#bytes)
8. [Durations](#durations)
9. [Useful queries](#useful-queries)

---

## Turning metrics on

kolbasa needs Micrometer 1.13 or newer, but doesn't bring it along. An application that reports metrics usually has it
already: Spring Boot, Quarkus and Micronaut all report through Micrometer. Without Micrometer, the default
`MetricsConfig.None` still works. Only `MetricsConfig.Config` needs it.

Turn metrics on once, at startup, with the registry your application reports from. In a Spring Boot application, that
is the `MeterRegistry` bean:

```kotlin
Kolbasa.metricsConfig = MetricsConfig.Config(meterRegistry)
```

There is no default registry, on purpose. Micrometer has a global one, `Metrics.globalRegistry`, but it only passes
metrics on to the registries added to it. With none added, it accepts kolbasa's metrics and sends them nowhere, without
an error. If you do use it, pass it explicitly: `MetricsConfig.Config(Metrics.globalRegistry)`.

The metrics of a queue are registered on the first call to that queue. You can create as many `Config` objects over
one registry as you like: they share the same metrics.

`Kolbasa.metricsConfig` is read on every call. A new value takes effect on the next call.

## Prometheus without Spring

Add the Micrometer registry for Prometheus. It brings Micrometer itself and the Prometheus Java client along:

```groovy
implementation "io.micrometer:micrometer-registry-prometheus:<version>"
```

Create the registry and give it to kolbasa:

```kotlin
val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
Kolbasa.metricsConfig = MetricsConfig.Config(registry)
```

Prometheus reads the metrics over HTTP, from `registry.scrape()`. How to serve it is up to your application, see
[the Micrometer docs](https://docs.micrometer.io/micrometer/reference/implementations/prometheus.html).

Your application may already write other metrics straight to the Prometheus client, to
`PrometheusRegistry.defaultRegistry`. Then build the Micrometer registry over it, and kolbasa's metrics come out of the
same endpoint as the rest:

```kotlin
val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)
```

## Per producer, consumer and mutator

`ProducerOptions`, `ConsumerOptions` and `MutatorOptions` have their own `metricsConfig`. It works the same way as
`tracing` there:

- `null`, the default: use `Kolbasa.metricsConfig`.
- A `Config`: use it instead, for example with another registry.
- `MetricsConfig.None`: no metrics for this producer, consumer or mutator, whatever the global setting is.

```kotlin
// This consumer reports nothing, even with metrics on globally
val consumer = DatabaseConsumer(dataSource, ConsumerOptions(metricsConfig = MetricsConfig.None))
```

Java has no named arguments, so there the options come from a builder:

```java
var options = ConsumerOptions.builder()
    .metricsConfig(MetricsConfig.None.INSTANCE)
    .build();

var consumer = new DatabaseConsumer(dataSource, options);
```

A consumer also reports the sweeps that its `receive()` runs. A sweep you run yourself with
`SweepHelper.sweep(connection, queue, limit)` reports to `Kolbasa.metricsConfig`. To report it somewhere else, pass a
config: `SweepHelper.sweep(connection, queue, limit, metricsConfig)`.

The inspector reports no metrics. `QueueWorker`, `QueueProcessor` and `QueuePipe` report through the producers and
consumers you give them.

## Metric reference

Every metric has two names. The Micrometer name, with dots, is the one to look up in a `MeterRegistry`. Backends other
than Prometheus show it too, each in its own naming style. The Prometheus name is the one dashboards and alerts use:
the dots become underscores, counters get `_total`, and the unit goes into the name.

A timer comes to Prometheus as a histogram, three series `_bucket`, `_count` and `_sum`, plus a gauge `_max`. See
[Durations](#durations).

Every metric has the tags `queue` and `node_id`. The **Tags** column shows only the others. See [Tags](#tags).

### Queue state

These metrics describe the queue table in the database, not your application. See
[How queue state is updated](#how-queue-state-is-updated).

| Micrometer | Prometheus | Type | Tags | Meaning |
|---|---|---|---|---|
| `kolbasa.queue.messages` | `kolbasa_queue_messages` | gauge | `state` | Messages in each [state](Message%20state%20transitions.md#states-description). Approximate. |
| `kolbasa.queue.message.age` | `kolbasa_queue_message_age_seconds` | gauge | `type` | `OLDEST`: how long ago the oldest message was sent. `NEWEST`: the same for the newest one. `OLDEST_READY`: how long the oldest message that can be received now has been waiting. `0` when there is no such message. |
| `kolbasa.queue.size` | `kolbasa_queue_size_bytes` | gauge | | Size of the queue table on disk in bytes, with its indexes and TOAST. |

### Producer

| Micrometer | Prometheus | Type | Tags | Meaning |
|---|---|---|---|---|
| `kolbasa.producer.send` | `kolbasa_producer_send_total` | counter | `partial_insert_type` | `send()` calls. |
| `kolbasa.producer.send.messages` | `kolbasa_producer_send_messages_total` | counter | `partial_insert_type` | Messages in these calls, sent or not. |
| `kolbasa.producer.send.messages.failed` | `kolbasa_producer_send_messages_failed_total` | counter | `partial_insert_type` | Messages that were not sent, as in `SendResult.failedMessages`. |
| `kolbasa.producer.send.bytes` | `kolbasa_producer_send_bytes_total` | counter | `partial_insert_type` | Payload bytes, see [Bytes](#bytes). |
| `kolbasa.producer.send.duration` | `kolbasa_producer_send_duration_seconds` | timer | `partial_insert_type` | How long `send()` calls take. |

### Consumer

| Micrometer | Prometheus | Type | Tags | Meaning |
|---|---|---|---|---|
| `kolbasa.consumer.receive` | `kolbasa_consumer_receive_total` | counter | | `receive()` calls, also those that got nothing. |
| `kolbasa.consumer.receive.messages` | `kolbasa_consumer_receive_messages_total` | counter | | Messages received. Unreadable ones are not counted here. |
| `kolbasa.consumer.receive.messages.unreadable` | `kolbasa_consumer_receive_messages_unreadable_total` | counter | | Received messages that couldn't be read and went to the `ReadErrorHandler`. |
| `kolbasa.consumer.receive.bytes` | `kolbasa_consumer_receive_bytes_total` | counter | | Payload bytes, see [Bytes](#bytes). |
| `kolbasa.consumer.receive.duration` | `kolbasa_consumer_receive_duration_seconds` | timer | | How long `receive()` calls take. |
| `kolbasa.consumer.delete` | `kolbasa_consumer_delete_total` | counter | | `delete()` calls. |
| `kolbasa.consumer.delete.messages` | `kolbasa_consumer_delete_messages_total` | counter | | Messages deleted. |
| `kolbasa.consumer.delete.duration` | `kolbasa_consumer_delete_duration_seconds` | timer | | How long `delete()` calls take. |

### Sweep

| Micrometer | Prometheus | Type | Tags | Meaning |
|---|---|---|---|---|
| `kolbasa.sweep` | `kolbasa_sweep_total` | counter | | [Sweep](Architecture.md#sweep) passes, also those that removed nothing. |
| `kolbasa.sweep.removed.messages` | `kolbasa_sweep_removed_messages_total` | counter | | DEAD messages removed from the queue: deleted, or moved to the DLQ. |
| `kolbasa.sweep.duration` | `kolbasa_sweep_duration_seconds` | timer | | How long the removal of DEAD messages takes. The retention cleanup of the DLQ and the archive is not measured. |

### Mutator

| Micrometer | Prometheus | Type | Tags | Meaning |
|---|---|---|---|---|
| `kolbasa.mutator.mutate` | `kolbasa_mutator_mutate_total` | counter | `type` | Mutate calls. |
| `kolbasa.mutator.mutate.iterations` | `kolbasa_mutator_mutate_iterations_total` | counter | `type` | Iterations. A mutate by id is one iteration, a mutate by filter goes through the queue in several. |
| `kolbasa.mutator.mutated.messages` | `kolbasa_mutator_mutated_messages_total` | counter | `type` | Messages changed. |
| `kolbasa.mutator.mutate.duration` | `kolbasa_mutator_mutate_duration_seconds` | timer | `type` | How long mutate calls take. |

## Tags

Prometheus calls them labels.

| Tag | Values |
|---|---|
| `queue` | The queue name, exactly as in `Queue.name`. The table name in the database is in lower case, the tag is not. |
| `node_id` | The node a cluster producer, consumer or mutator works with, see [Cluster architecture.md](Cluster%20architecture.md). Empty for the usual ones that work with one database. |
| `partial_insert_type` | The partial insert mode of the call: `PROHIBITED`, `UNTIL_FIRST_FAILURE`, `INSERT_AS_MANY_AS_POSSIBLE`. |
| `state` | `SCHEDULED`, `READY`, `IN_FLIGHT`, `RETRY`, `DEAD`, see [Message state transitions.md](Message%20state%20transitions.md#states-description). There is no `COMPLETED`: those messages have left the queue. |
| `type` | For message ages: `OLDEST`, `NEWEST`, `OLDEST_READY`. For the mutator: `by_id`, `by_filter`. |

## How queue state is updated

The queue state metrics come from the database. kolbasa doesn't run a timer for them.

After a producer, consumer, mutator or sweep call, kolbasa checks when it last read the state of this queue. If it was
more than a minute ago, it reads them again. These are a few small queries: the inspector's `count()`, `size()` and
`messageAge()`. They run on the same connection, in the same transaction as the call. Only one thread reads at a time,
the others don't wait for it.

What follows from this:

- The values can be up to a minute old.
- A queue nobody works with keeps its last values. After a restart it has none until the first call.
- Message counts are approximate: `count()` reads a sample of the table. See [Inspector](Architecture.md#inspector).
- Every instance of your application that works with a queue reports the same values for it. Aggregate these metrics
  with `max`, not `sum`.

## Bytes

`kolbasa_producer_send_bytes_total` and `kolbasa_consumer_receive_bytes_total` count message payloads only. How a
payload is counted depends on its [data type](Architecture.md#payload-data-types):

- Text and JSON: the string in UTF-8 bytes.
- Binary: the bytes as they are.
- `Int`: 4 bytes, `Long`: 8 bytes.

Meta-fields, ids and the rest of the row are not counted. For JSON this is the size of the JSON string, not of what
PostgreSQL stores on disk.

## Durations

All durations are Micrometer timers. In Prometheus they are histograms in seconds, with these buckets:

- 1, 2, 5, 10, 20, 50, 100, 200 and 500 milliseconds;
- 1, 2, 5, 10 and 30 seconds.

To use other buckets, set `serviceLevelObjectives` for the meters that start with `kolbasa.` in a Micrometer
`MeterFilter`. In Spring Boot, the same is a property: `management.metrics.distribution.slo.kolbasa=10ms,100ms,1s`.

The `_max` gauge is the longest call in a short sliding window, two minutes by default. So it goes down again when the
calls get faster.

A duration covers the whole call: turning messages into SQL parameters or rows into messages, and the work in the
database. The sweep that `receive()` sometimes runs is not part of the receive duration. It has its own metrics.

## Useful queries

These queries are for Prometheus, in PromQL, with the Prometheus names from the [Metric reference](#metric-reference).

Messages sent per second, by queue:

```promql
sum by (queue) (rate(kolbasa_producer_send_messages_total[5m]))
```

Messages received per second, by queue:

```promql
sum by (queue) (rate(kolbasa_consumer_receive_messages_total[5m]))
```

Messages in each state. These metrics come from the database, so use `max`, not `sum`:

```promql
max by (queue, state) (kolbasa_queue_messages)
```

In a cluster, each node has its own part of the queue. Take `max` per node, then add the nodes up:

```promql
sum by (queue, state) (max by (queue, state, node_id) (kolbasa_queue_messages))
```

Alert: a message that can be received has been waiting for more than 5 minutes:

```promql
max by (queue) (kolbasa_queue_message_age_seconds{type="OLDEST_READY"}) > 300
```

Alert: some messages can't be read:

```promql
sum by (queue) (increase(kolbasa_consumer_receive_messages_unreadable_total[10m])) > 0
```

99th percentile of `receive()` duration:

```promql
histogram_quantile(0.99, sum by (queue, le) (rate(kolbasa_consumer_receive_duration_seconds_bucket[5m])))
```
