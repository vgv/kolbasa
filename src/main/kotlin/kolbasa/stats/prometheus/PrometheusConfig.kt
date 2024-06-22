package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Gauge
import io.prometheus.metrics.core.metrics.Histogram
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kolbasa.queue.Checks
import kolbasa.queue.Queue
import kolbasa.stats.prometheus.metrics.Const
import java.time.Duration

sealed class PrometheusConfig {

    object None : PrometheusConfig()

    data class Config(
        /**
         * By default, the size of the string to send to Prometheus is calculated in characters, not in bytes, which is much
         * faster. If you really need to calculate exact string size in bytes, set this option to true (but it's much slower).
         */
        val preciseStringSize: Boolean = false,

        /**
         * By default, kolbasa measures the queue size every [PrometheusConfig.DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL],
         * however, if you want to measure more/less often for specific queues – you can
         * change the default measurement interval here.
         *
         * Please note that current implementation relies on PostgreSQL vacuum statistics, so, if you don't run vacuum every
         * five seconds (for example) it's almost nonsense to measure the queue size every five seconds.
         *
         * Map key: Queue name
         * Map value: Interval
         */
        val customQueueSizeMeasureInterval: Map<String, Duration> = emptyMap(),

        /**
         * Prometheus registry. By default, [PrometheusRegistry.defaultRegistry] is used.
         * You can use your custom registry, but you need to change the registry before any
         * other queue operations (at application startup, for example).
         */
        val registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry
    ) : PrometheusConfig() {

        // ----------------------------------------------------------------------------
        // Producer metrics
        internal val producerSendCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send")
            .help("Amount of producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(registry)

        internal val producerSendRowsCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_rows")
            .help("Amount of all (successful and failed) rows sent by producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(registry)

        internal val producerSendFailedRowsCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_rows_failed")
            .help("Amount of failed rows sent by producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(registry)

        internal val producerSendDuration: Histogram = Histogram.builder()
            .name("kolbasa_producer_send_duration_seconds")
            .help("Producer send() calls duration")
            .labelNames("queue", "partial_insert_type")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(registry)

        internal val producerSendBytesCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_bytes")
            .help("Amount of bytes sent by producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(registry)

        internal val producerQueueSizeGauge: Gauge = Gauge.builder()
            .name("kolbasa_producer_queue_size")
            .help("Producer queue size")
            .labelNames("queue")
            .register(registry)

        // ----------------------------------------------------------------------------
        // Consumer metrics
        // Receive
        internal val consumerReceiveCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive")
            .help("Amount of consumer receive() calls")
            .labelNames("queue")
            .register(registry)

        internal val consumerReceiveBytesCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive_bytes")
            .help("Amount of bytes read by consumer receive() calls")
            .labelNames("queue")
            .register(registry)

        internal val consumerReceiveRowsCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive_rows")
            .help("Amount of rows received by consumer receive() calls")
            .labelNames("queue")
            .register(registry)

        internal val consumerReceiveDuration: Histogram = Histogram.builder()
            .name("kolbasa_consumer_receive_duration_seconds")
            .help("Consumer receive() calls duration")
            .labelNames("queue")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(registry)

        // Delete
        internal val consumerDeleteCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_delete")
            .help("Amount of consumer delete() calls")
            .labelNames("queue")
            .register(registry)

        internal val consumerDeleteRowsCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_delete_rows")
            .help("Amount of rows removed by consumer delete() calls")
            .labelNames("queue")
            .register(registry)

        internal val consumerDeleteDuration: Histogram = Histogram.builder()
            .name("kolbasa_consumer_delete_duration_seconds")
            .help("Consumer delete() calls duration")
            .labelNames("queue")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(registry)

        internal val consumerQueueSizeGauge: Gauge = Gauge.builder()
            .name("kolbasa_consumer_queue_size")
            .help("Consumer queue size")
            .labelNames("queue")
            .register(registry)

        // ----------------------------------------------------------------------------
        // Sweep metrics
        internal val sweepCounter: Counter = Counter.builder()
            .name("kolbasa_sweep")
            .help("Number of sweeps")
            .labelNames("queue")
            .register(registry)

        internal val sweepIterationsCounter: Counter = Counter.builder()
            .name("kolbasa_sweep_iterations")
            .help("Number of sweep iterations (every sweep can have multiple iterations)")
            .labelNames("queue")
            .register(registry)

        internal val sweepRowsRemovedCounter: Counter = Counter.builder()
            .name("kolbasa_sweep_removed_rows")
            .help("Number of rows removed by sweep")
            .labelNames("queue")
            .register(registry)

        internal val sweepDuration: Histogram = Histogram.builder()
            .name("kolbasa_sweep_duration_seconds")
            .help("Sweep duration")
            .labelNames("queue")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(registry)


        init {
            customQueueSizeMeasureInterval.forEach { (queueName, customInterval) ->
                Checks.checkCustomQueueSizeMeasureInterval(queueName, customInterval)
            }
        }

        class Builder internal constructor() {
            private var preciseStringSize: Boolean = false
            private var customQueueSizeMeasureInterval: MutableMap<String, Duration> = mutableMapOf()
            private var registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry

            fun customQueueSizeMeasureInterval(queueName: String, customInterval: Duration) = apply {
                customQueueSizeMeasureInterval[queueName] = customInterval
            }

            fun customQueueSizeMeasureInterval(queue: Queue<*, *>, customInterval: Duration) =
                customQueueSizeMeasureInterval(queue.name, customInterval)

            fun preciseStringSize(preciseStringSize: Boolean) = apply { this.preciseStringSize = preciseStringSize }
            fun registry(registry: PrometheusRegistry) = apply { this.registry = registry }

            fun build() = Config(preciseStringSize, customQueueSizeMeasureInterval, registry)
        }

        companion object {
            internal val MIN_QUEUE_SIZE_MEASURE_INTERVAL = Duration.ofSeconds(1)
            internal val DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL = Duration.ofSeconds(15)

            @JvmStatic
            fun builder(): Builder = Builder()
        }
    }
}

