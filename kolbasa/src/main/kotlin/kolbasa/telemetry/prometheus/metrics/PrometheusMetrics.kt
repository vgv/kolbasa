package kolbasa.telemetry.prometheus.metrics

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Gauge
import io.prometheus.metrics.core.metrics.Histogram
import io.prometheus.metrics.model.registry.PrometheusRegistry
import java.util.Collections
import java.util.WeakHashMap

/**
 * All metrics of the library, registered in one registry.
 *
 * A registry refuses a metric name it already holds, so there is one such object per registry: [of] makes it on the
 * first call and returns the same object after that.
 */
internal class PrometheusMetrics private constructor(registry: PrometheusRegistry) {

    // ----------------------------------------------------------------------------
    // Queue metrics
    val queueMessagesGauge: Gauge = Gauge.builder()
        .name("kolbasa_queue_messages")
        .help("Amount of messages in the queue in different states (scheduled, ready, in-flight, retry, dead)")
        .labelNames("queue", "state", "node_id")
        .register(registry)

    val queueMessageAgesGauge: Gauge = Gauge.builder()
        .name("kolbasa_queue_message_age_seconds")
        .help("Queue messages ages in seconds – oldest, newest and oldest ready")
        .labelNames("queue", "type", "node_id")
        .register(registry)

    val queueSizeGauge: Gauge = Gauge.builder()
        .name("kolbasa_queue_size")
        .help("Queue size in bytes")
        .labelNames("queue", "node_id")
        .register(registry)

    // ----------------------------------------------------------------------------
    // Producer metrics
    val producerSendCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send")
        .help("Amount of producer send() calls")
        .labelNames("queue", "partial_insert_type", "node_id")
        .register(registry)

    val producerSendMessagesCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send_messages")
        .help("Amount of all (successful and failed) messages sent by producer send() calls")
        .labelNames("queue", "partial_insert_type", "node_id")
        .register(registry)

    val producerSendFailedMessagesCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send_messages_failed")
        .help("Amount of failed messages sent by producer send() calls")
        .labelNames("queue", "partial_insert_type", "node_id")
        .register(registry)

    val producerSendDuration: Histogram = Histogram.builder()
        .name("kolbasa_producer_send_duration_seconds")
        .help("Producer send() calls duration")
        .labelNames("queue", "partial_insert_type", "node_id")
        .classicOnly()
        .classicUpperBounds(*Const.callsDurationHistogramBuckets())
        .register(registry)

    val producerSendBytesCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send_bytes")
        .help("Amount of bytes sent by producer send() calls")
        .labelNames("queue", "partial_insert_type", "node_id")
        .register(registry)

    // ----------------------------------------------------------------------------
    // Consumer metrics
    // Receive
    val consumerReceiveCounter: Counter = Counter.builder()
        .name("kolbasa_consumer_receive")
        .help("Amount of consumer receive() calls")
        .labelNames("queue", "node_id")
        .register(registry)

    val consumerReceiveBytesCounter: Counter = Counter.builder()
        .name("kolbasa_consumer_receive_bytes")
        .help("Amount of bytes read by consumer receive() calls")
        .labelNames("queue", "node_id")
        .register(registry)

    val consumerReceiveMessagesCounter: Counter = Counter.builder()
        .name("kolbasa_consumer_receive_messages")
        .help("Amount of messages received by consumer receive() calls")
        .labelNames("queue", "node_id")
        .register(registry)

    val consumerReceiveUnreadableMessagesCounter: Counter = Counter.builder()
        .name("kolbasa_consumer_receive_messages_unreadable")
        .help("Amount of received messages that can't be read and were passed to the ReadErrorHandler")
        .labelNames("queue", "node_id")
        .register(registry)

    val consumerReceiveDuration: Histogram = Histogram.builder()
        .name("kolbasa_consumer_receive_duration_seconds")
        .help("Consumer receive() calls duration")
        .labelNames("queue", "node_id")
        .classicOnly()
        .classicUpperBounds(*Const.callsDurationHistogramBuckets())
        .register(registry)

    // Delete
    val consumerDeleteCounter: Counter = Counter.builder()
        .name("kolbasa_consumer_delete")
        .help("Amount of consumer delete() calls")
        .labelNames("queue", "node_id")
        .register(registry)

    val consumerDeleteMessagesCounter: Counter = Counter.builder()
        .name("kolbasa_consumer_delete_messages")
        .help("Amount of messages removed by consumer delete() calls")
        .labelNames("queue", "node_id")
        .register(registry)

    val consumerDeleteDuration: Histogram = Histogram.builder()
        .name("kolbasa_consumer_delete_duration_seconds")
        .help("Consumer delete() calls duration")
        .labelNames("queue", "node_id")
        .classicOnly()
        .classicUpperBounds(*Const.callsDurationHistogramBuckets())
        .register(registry)

    // ----------------------------------------------------------------------------
    // Sweep metrics
    val sweepCounter: Counter = Counter.builder()
        .name("kolbasa_sweep")
        .help("Number of sweeps")
        .labelNames("queue", "node_id")
        .register(registry)

    val sweepMessagesRemovedCounter: Counter = Counter.builder()
        .name("kolbasa_sweep_removed_messages")
        .help("Number of messages removed by sweep")
        .labelNames("queue", "node_id")
        .register(registry)

    val sweepDuration: Histogram = Histogram.builder()
        .name("kolbasa_sweep_duration_seconds")
        .help("Sweep duration")
        .labelNames("queue", "node_id")
        .classicOnly()
        .classicUpperBounds(*Const.callsDurationHistogramBuckets())
        .register(registry)

    // ----------------------------------------------------------------------------
    // Mutator metrics
    val mutatorMutateCounter: Counter = Counter.builder()
        .name("kolbasa_mutator_mutate")
        .help("Number of mutates")
        .labelNames("queue", "node_id", "type")
        .register(registry)

    val mutatorMutateIterationsCounter: Counter = Counter.builder()
        .name("kolbasa_mutator_mutate_iterations")
        .help("Number of mutate iterations (every mutate can have multiple iterations)")
        .labelNames("queue", "node_id", "type")
        .register(registry)

    val mutatorMutateMessagesCounter: Counter = Counter.builder()
        .name("kolbasa_mutator_mutated_messages")
        .help("Number of mutated messages")
        .labelNames("queue", "node_id", "type")
        .register(registry)

    val mutatorMutateDuration: Histogram = Histogram.builder()
        .name("kolbasa_mutator_mutate_duration_seconds")
        .help("Mutate duration")
        .labelNames("queue", "node_id", "type")
        .classicOnly()
        .classicUpperBounds(*Const.callsDurationHistogramBuckets())
        .register(registry)

    companion object {

        // Weak keys: a registry nobody uses any more is not kept in memory by this map
        private val byRegistry: MutableMap<PrometheusRegistry, PrometheusMetrics> =
            Collections.synchronizedMap(WeakHashMap())

        fun of(registry: PrometheusRegistry): PrometheusMetrics {
            return byRegistry.computeIfAbsent(registry) { PrometheusMetrics(it) }
        }
    }
}
