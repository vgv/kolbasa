package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Gauge
import io.prometheus.metrics.core.metrics.Histogram
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kolbasa.stats.prometheus.metrics.Const

/**
 * Says whether Kolbasa collects Prometheus metrics, and where it registers them.
 *
 * There are two states, and nothing in between: [None] collects nothing, [Config] collects everything – message
 * counters, queue sizes, message ages, call durations. [None] is the default, so metrics cost you nothing until you
 * ask for them.
 *
 * Set it through [Kolbasa.prometheusConfig][kolbasa.Kolbasa.prometheusConfig] at application startup, before you
 * create your queues and start sending. Any later changes will have no effect.
 *
 * ## Usage Example
 *
 * ```kotlin
 * Kolbasa.prometheusConfig = PrometheusConfig.Config()
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setPrometheusConfig(new PrometheusConfig.Config());
 * ```
 */
sealed class PrometheusConfig {

    /** No metrics at all: nothing is measured and nothing is registered in any registry. */
    object None : PrometheusConfig()

    /**
     * Metrics, registered in a Prometheus registry.
     *
     * Every metric of the library is created and registered when this object is created, not when the first message
     * is sent. That is a deliberate trade: the work happens once, and reporting a value later costs almost nothing.
     *
     * It also means one such object per registry. A registry refuses a metric name it already holds, so building a
     * second [Config] over the same registry fails with `duplicate metric name`. Build one, give it to
     * [Kolbasa.prometheusConfig][kolbasa.Kolbasa.prometheusConfig], and keep it.
     */
    data class Config(
        /**
         * By default, the size of the string to send to Prometheus is calculated in characters, not in bytes, which is much
         * faster. If you really need to calculate exact string size in bytes, set this option to true (but it's much slower).
         */
        val preciseStringSize: Boolean = false,

        /**
         * Prometheus registry. By default, [PrometheusRegistry.defaultRegistry] is used.
         * You can use your custom registry, but you need to change the registry before any
         * other queue operations (at application startup, for example).
         */
        val registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry
    ) : PrometheusConfig() {

        // ----------------------------------------------------------------------------
        // Queue metrics
        internal val queueMessagesGauge: Gauge = Gauge.builder()
            .name("kolbasa_queue_messages")
            .help("Amount of messages in the queue in different states (scheduled, ready, in-flight, retry, dead)")
            .labelNames("queue", "state", "node_id")
            .register(registry)

        internal val queueMessageAgesGauge: Gauge = Gauge.builder()
            .name("kolbasa_queue_message_age_seconds")
            .help("Queue messages ages in seconds – oldest, newest and oldest ready")
            .labelNames("queue", "type", "node_id")
            .register(registry)

        internal val queueSizeGauge: Gauge = Gauge.builder()
            .name("kolbasa_queue_size")
            .help("Queue size in bytes")
            .labelNames("queue", "node_id")
            .register(registry)

        // ----------------------------------------------------------------------------
        // Producer metrics
        internal val producerSendCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send")
            .help("Amount of producer send() calls")
            .labelNames("queue", "partial_insert_type", "node_id")
            .register(registry)

        internal val producerSendMessagesCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_messages")
            .help("Amount of all (successful and failed) messages sent by producer send() calls")
            .labelNames("queue", "partial_insert_type", "node_id")
            .register(registry)

        internal val producerSendFailedMessagesCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_messages_failed")
            .help("Amount of failed messages sent by producer send() calls")
            .labelNames("queue", "partial_insert_type", "node_id")
            .register(registry)

        internal val producerSendDuration: Histogram = Histogram.builder()
            .name("kolbasa_producer_send_duration_seconds")
            .help("Producer send() calls duration")
            .labelNames("queue", "partial_insert_type", "node_id")
            .classicOnly()
            .classicUpperBounds(*Const.callsDurationHistogramBuckets())
            .register(registry)

        internal val producerSendBytesCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_bytes")
            .help("Amount of bytes sent by producer send() calls")
            .labelNames("queue", "partial_insert_type", "node_id")
            .register(registry)

        // ----------------------------------------------------------------------------
        // Consumer metrics
        // Receive
        internal val consumerReceiveCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive")
            .help("Amount of consumer receive() calls")
            .labelNames("queue", "node_id")
            .register(registry)

        internal val consumerReceiveBytesCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive_bytes")
            .help("Amount of bytes read by consumer receive() calls")
            .labelNames("queue", "node_id")
            .register(registry)

        internal val consumerReceiveMessagesCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive_messages")
            .help("Amount of messages received by consumer receive() calls")
            .labelNames("queue", "node_id")
            .register(registry)

        internal val consumerReceiveDuration: Histogram = Histogram.builder()
            .name("kolbasa_consumer_receive_duration_seconds")
            .help("Consumer receive() calls duration")
            .labelNames("queue", "node_id")
            .classicOnly()
            .classicUpperBounds(*Const.callsDurationHistogramBuckets())
            .register(registry)

        // Delete
        internal val consumerDeleteCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_delete")
            .help("Amount of consumer delete() calls")
            .labelNames("queue", "node_id")
            .register(registry)

        internal val consumerDeleteMessagesCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_delete_messages")
            .help("Amount of messages removed by consumer delete() calls")
            .labelNames("queue", "node_id")
            .register(registry)

        internal val consumerDeleteDuration: Histogram = Histogram.builder()
            .name("kolbasa_consumer_delete_duration_seconds")
            .help("Consumer delete() calls duration")
            .labelNames("queue", "node_id")
            .classicOnly()
            .classicUpperBounds(*Const.callsDurationHistogramBuckets())
            .register(registry)

        // ----------------------------------------------------------------------------
        // Sweep metrics
        internal val sweepCounter: Counter = Counter.builder()
            .name("kolbasa_sweep")
            .help("Number of sweeps")
            .labelNames("queue", "node_id")
            .register(registry)

        internal val sweepMessagesRemovedCounter: Counter = Counter.builder()
            .name("kolbasa_sweep_removed_messages")
            .help("Number of messages removed by sweep")
            .labelNames("queue", "node_id")
            .register(registry)

        internal val sweepDuration: Histogram = Histogram.builder()
            .name("kolbasa_sweep_duration_seconds")
            .help("Sweep duration")
            .labelNames("queue", "node_id")
            .classicOnly()
            .classicUpperBounds(*Const.callsDurationHistogramBuckets())
            .register(registry)

        // ----------------------------------------------------------------------------
        // Mutator metrics
        internal val mutatorMutateCounter: Counter = Counter.builder()
            .name("kolbasa_mutator_mutate")
            .help("Number of mutates")
            .labelNames("queue", "node_id", "type")
            .register(registry)

        internal val mutatorMutateIterationsCounter: Counter = Counter.builder()
            .name("kolbasa_mutator_mutate_iterations")
            .help("Number of mutate iterations (every mutate can have multiple iterations)")
            .labelNames("queue", "node_id", "type")
            .register(registry)

        internal val mutatorMutateMessagesCounter: Counter = Counter.builder()
            .name("kolbasa_mutator_mutated_messages")
            .help("Number of mutated messages")
            .labelNames("queue", "node_id", "type")
            .register(registry)

        internal val mutatorMutateDuration: Histogram = Histogram.builder()
            .name("kolbasa_mutator_mutate_duration_seconds")
            .help("Mutate duration")
            .labelNames("queue", "node_id", "type")
            .classicOnly()
            .classicUpperBounds(*Const.callsDurationHistogramBuckets())
            .register(registry)

        /** Builder for flexible [Config] creation, when only some of the properties need to be set. */
        class Builder internal constructor() {
            private var preciseStringSize: Boolean = false
            private var registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry

            /** Sets [Config.preciseStringSize] – whether message sizes are measured in bytes instead of characters. */
            fun preciseStringSize(preciseStringSize: Boolean) = apply { this.preciseStringSize = preciseStringSize }

            /** Sets [Config.registry] – the Prometheus registry all metrics are registered in. */
            fun registry(registry: PrometheusRegistry) = apply { this.registry = registry }

            /**
             * Creates a new [Config] instance: a fresh one on every call, nothing is cached or reused.
             *
             * Every call also registers the whole set of Kolbasa metrics in the chosen registry, so call it once –
             * see [Config].
             */
            fun build() = Config(preciseStringSize, registry)
        }

        companion object {
            @JvmStatic
            fun builder(): Builder = Builder()
        }
    }
}

