package kolbasa.stats.prometheus

import io.prometheus.metrics.model.registry.PrometheusRegistry
import kolbasa.queue.Checks
import kolbasa.queue.Queue
import java.time.Duration

data class PrometheusConfig(
    /**
     * Collect Prometheus metrics or not.
     */
    val enabled: Boolean = false,

    /**
     * By default, the size of the string to send to Prometheus is calculated in characters, not in bytes, which is much
     * faster. If you really need to calculate exact string size in bytes, set this option to true (but it's much slower).
     */
    val preciseStringSize: Boolean = false,

    /**
     * By default, kolbasa measures the queue size every [PrometheusConfig.DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL],
     * however, if you want to measure more/less often for specific queues – you can
     * change the default measurement interval here.
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
) {

    init {
        customQueueSizeMeasureInterval.forEach { (queueName, customInterval) ->
            Checks.checkCustomQueueSizeMeasureInterval(queueName, customInterval)
        }
    }

    class Builder internal constructor() {
        private var enabled: Boolean = false
        private var preciseStringSize: Boolean = false
        private var customQueueSizeMeasureInterval: MutableMap<String, Duration> = mutableMapOf()
        private var registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry

        fun enabled() = apply { this.enabled = true }
        fun disabled() = apply { this.enabled = false }
        fun customQueueSizeMeasureInterval(queueName: String, customInterval: Duration) = apply {
            customQueueSizeMeasureInterval[queueName] = customInterval
        }

        fun customQueueSizeMeasureInterval(queue: Queue<*, *>, customInterval: Duration) =
            customQueueSizeMeasureInterval(queue.name, customInterval)

        fun preciseStringSize(preciseStringSize: Boolean) = apply { this.preciseStringSize = preciseStringSize }
        fun registry(registry: PrometheusRegistry) = apply { this.registry = registry }

        fun build() = PrometheusConfig(enabled, preciseStringSize, customQueueSizeMeasureInterval, registry)
    }

    companion object {

        internal val MIN_QUEUE_SIZE_MEASURE_INTERVAL = Duration.ofSeconds(1)
        internal val DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL = Duration.ofSeconds(15)

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
