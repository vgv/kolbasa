# Prometheus metrics

kolbasa can report what it does to [Prometheus](https://prometheus.io/): how many messages your producers send, how many
your consumers receive and delete, how long each call takes, and how big and how old each queue is.

Metrics are off by default. They cost nothing until you turn them on.

## Table of Contents

1. [Turning metrics on](#turning-metrics-on)
2. [Per producer, consumer and mutator](#per-producer-consumer-and-mutator)
3. [Metric reference](#metric-reference)
4. [Labels](#labels)
5. [How queue state is updated](#how-queue-state-is-updated)
6. [Bytes](#bytes)
7. [Durations](#durations)
8. [Useful queries](#useful-queries)

---

## Turning metrics on

kolbasa uses the Prometheus Java client, [client_java](https://github.com/prometheus/client_java) 1.x, but doesn't bring
it along. Add it yourself, with a 1.x version:

```groovy
implementation "io.prometheus:prometheus-metrics-core:<version>"
```

Without it, the default `PrometheusConfig.None` still works. Only `PrometheusConfig.Config` needs the library.

Turn metrics on once, at startup:

```kotlin
Kolbasa.prometheusConfig = PrometheusConfig.Config()
```

The metrics go to `PrometheusRegistry.defaultRegistry`. To use another registry, pass it:

```kotlin
Kolbasa.prometheusConfig = PrometheusConfig.Config(registry = myRegistry)
```

kolbasa only puts metrics into a registry. Prometheus reads them through an exporter, an HTTP endpoint in your
application. If your application already reports to Prometheus, it has an exporter, and there is nothing more to set
up. Only check that the exporter serves the registry kolbasa writes to: the default one, or the one you passed to
`Config`.

To just see the metrics, for example on your machine, add the HTTP server from client_java, with the same version as
`prometheus-metrics-core`:

```groovy
implementation "io.prometheus:prometheus-metrics-exporter-httpserver:<version>"
```

Start it, then run some code that sends and receives messages in a loop:

```kotlin
HTTPServer.builder().port(9400).buildAndStart()
```

Open `http://localhost:9400/metrics` in a browser. kolbasa's metrics start with `kolbasa_`.

All metrics are registered when the first `Config` over a registry is created. You can create as many `Config` objects
over one registry as you like: they share the same metrics.

`Kolbasa.prometheusConfig` is read on every call. A new value takes effect on the next call.

## Per producer, consumer and mutator

`ProducerOptions`, `ConsumerOptions` and `MutatorOptions` have their own `prometheusConfig`. It works the same way as
`tracing` there:

- `null`, the default: use `Kolbasa.prometheusConfig`.
- A `Config`: use it instead, for example with another registry.
- `PrometheusConfig.None`: no metrics for this producer, consumer or mutator, whatever the global setting is.

```kotlin
// This consumer reports nothing, even with metrics on globally
val consumer = DatabaseConsumer(dataSource, ConsumerOptions(prometheusConfig = PrometheusConfig.None))
```

Java has no named arguments, so there the options come from a builder:

```java
var options = ConsumerOptions.builder()
    .prometheusConfig(PrometheusConfig.None.INSTANCE)
    .build();

var consumer = new DatabaseConsumer(dataSource, options);
```

A consumer also reports the sweeps that its `receive()` runs. A sweep you run yourself with
`SweepHelper.sweep(connection, queue, limit)` reports to `Kolbasa.prometheusConfig`. To report it somewhere else, pass a
config: `SweepHelper.sweep(connection, queue, limit, prometheusConfig)`.

The inspector reports no metrics. `QueueWorker`, `QueueProcessor` and `QueuePipe` report through the producers and
consumers you give them.

## Metric reference

Counters get `_total` at the end of the name, as in every Prometheus client. The tables show the names as Prometheus
sees them. A histogram comes as three series: `_bucket`, `_count` and `_sum`. See [Durations](#durations).

Every metric has the labels `queue` and `node_id`. The **Labels** column shows only the others. See [Labels](#labels).

### Queue state

These metrics describe the queue table in the database, not your application. See
[How queue state is updated](#how-queue-state-is-updated).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kolbasa_queue_messages` | gauge | `state` | Messages in each [state](Message%20state%20transitions.md#states-description). Approximate. |
| `kolbasa_queue_message_age_seconds` | gauge | `type` | `OLDEST`: how long ago the oldest message was sent. `NEWEST`: the same for the newest one. `OLDEST_READY`: how long the oldest message that can be received now has been waiting. `0` when there is no such message. |
| `kolbasa_queue_size` | gauge | | Size of the queue table on disk in bytes, with its indexes and TOAST. |

### Producer

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kolbasa_producer_send_total` | counter | `partial_insert_type` | `send()` calls. |
| `kolbasa_producer_send_messages_total` | counter | `partial_insert_type` | Messages in these calls, sent or not. |
| `kolbasa_producer_send_messages_failed_total` | counter | `partial_insert_type` | Messages that were not sent, as in `SendResult.failedMessages`. |
| `kolbasa_producer_send_bytes_total` | counter | `partial_insert_type` | Payload bytes, see [Bytes](#bytes). |
| `kolbasa_producer_send_duration_seconds` | histogram | `partial_insert_type` | How long `send()` calls take. |

### Consumer

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kolbasa_consumer_receive_total` | counter | | `receive()` calls, also those that got nothing. |
| `kolbasa_consumer_receive_messages_total` | counter | | Messages received. Unreadable ones are not counted here. |
| `kolbasa_consumer_receive_messages_unreadable_total` | counter | | Received messages that couldn't be read and went to the `ReadErrorHandler`. |
| `kolbasa_consumer_receive_bytes_total` | counter | | Payload bytes, see [Bytes](#bytes). |
| `kolbasa_consumer_receive_duration_seconds` | histogram | | How long `receive()` calls take. |
| `kolbasa_consumer_delete_total` | counter | | `delete()` calls. |
| `kolbasa_consumer_delete_messages_total` | counter | | Messages deleted. |
| `kolbasa_consumer_delete_duration_seconds` | histogram | | How long `delete()` calls take. |

### Sweep

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kolbasa_sweep_total` | counter | | [Sweep](Architecture.md#sweep) passes, also those that removed nothing. |
| `kolbasa_sweep_removed_messages_total` | counter | | DEAD messages removed from the queue: deleted, or moved to the DLQ. |
| `kolbasa_sweep_duration_seconds` | histogram | | How long the removal of DEAD messages takes. The retention cleanup of the DLQ and the archive is not measured. |

### Mutator

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kolbasa_mutator_mutate_total` | counter | `type` | Mutate calls. |
| `kolbasa_mutator_mutate_iterations_total` | counter | `type` | Iterations. A mutate by id is one iteration, a mutate by filter goes through the queue in several. |
| `kolbasa_mutator_mutated_messages_total` | counter | `type` | Messages changed. |
| `kolbasa_mutator_mutate_duration_seconds` | histogram | `type` | How long mutate calls take. |

## Labels

| Label | Values |
|---|---|
| `queue` | The queue name, exactly as in `Queue.name`. The table name in the database is in lower case, the label is not. |
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

`kolbasa_producer_send_bytes_total` and `kolbasa_consumer_receive_bytes_total` count message payloads only. How a payload
is counted depends on its [data type](Architecture.md#payload-data-types):

- Text and JSON: the string in UTF-8 bytes.
- Binary: the bytes as they are.
- `Int`: 4 bytes, `Long`: 8 bytes.

Meta-fields, ids and the rest of the row are not counted. For JSON this is the size of the JSON string, not of what
PostgreSQL stores on disk.

## Durations

All durations are classic histograms in seconds. The buckets are:

- 1, 2, 5, 10, 20, 50, 100, 200 and 500 milliseconds;
- 1, 2, 5, 10 and 30 seconds.

A duration covers the whole call: turning messages into SQL parameters or rows into messages, and the work in the
database. The sweep that `receive()` sometimes runs is not part of the receive duration. It has its own metrics.

## Useful queries

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
