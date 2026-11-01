package kolbasa.telemetry.prometheus

import io.prometheus.metrics.model.registry.PrometheusRegistry
import kolbasa.queue.Queue
import kolbasa.telemetry.prometheus.metrics.PrometheusMetrics
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * Says whether Kolbasa collects Prometheus metrics, and where it registers them.
 *
 * There are two states, and nothing in between: [None] collects nothing, [Config] collects everything – message
 * counters, queue sizes, message ages, call durations. [None] is the default, so metrics cost you nothing until you
 * ask for them.
 *
 * Set it for the whole application with [Kolbasa.prometheusConfig][kolbasa.Kolbasa.prometheusConfig], or for one
 * producer, consumer or mutator in its options, for example
 * [ProducerOptions.prometheusConfig][kolbasa.producer.ProducerOptions.prometheusConfig]. A change takes effect on
 * the next call.
 *
 * ## Usage Example
 *
 * ```kotlin
 * Kolbasa.prometheusConfig = PrometheusConfig.Config()
 * // or if you don't want any metrics at all:
 * Kolbasa.prometheusConfig = PrometheusConfig.None;
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setPrometheusConfig(new PrometheusConfig.Config());
 * // or if you don't want any metrics at all:
 * Kolbasa.setPrometheusConfig(PrometheusConfig.None.INSTANCE);
 * ```
 */
sealed class PrometheusConfig {

    // Metrics of one queue: what every call reports to
    internal abstract fun queueMetrics(queue: Queue<*>): QueueMetrics

    /**
     * No metrics at all: nothing is measured and nothing is registered in any registry.
     */
    object None : PrometheusConfig() {
        override fun queueMetrics(queue: Queue<*>): QueueMetrics = EmptyQueueMetrics
    }

    /**
     * Metrics, registered in a Prometheus registry.
     *
     * Every metric of the library is created and registered when the first [Config] over a registry is created, not
     * when the first message is sent. That is a deliberate trade: the work happens once, and reporting a value later
     * costs almost nothing.
     *
     * You can build as many [Config] objects over one registry as you like: they share the same metrics. So
     * [Kolbasa.prometheusConfig][kolbasa.Kolbasa.prometheusConfig] and the options of a producer, consumer or mutator
     * can each have their own [Config], for example with a different [preciseStringSize].
     */
    data class Config(
        /**
         * By default, the size of the string to send to Prometheus is calculated in characters, not in bytes, which is much
         * faster. If you really need to calculate exact string size in bytes, set this option to true (but it's much slower).
         */
        val preciseStringSize: Boolean = false,

        /**
         * Prometheus registry. By default, [PrometheusRegistry.defaultRegistry] is used.
         */
        val registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry
    ) : PrometheusConfig() {

        // All metrics of the library, shared by every Config over the same registry
        internal val metrics: PrometheusMetrics = PrometheusMetrics.of(registry)

        // Performance: all metrics of a queue are made once, with their labels (queue name etc.), and kept here
        // Recommendation: https://prometheus.github.io/client_java/getting-started/performance/
        private val queues: ConcurrentMap<String, PrometheusQueueMetrics> = ConcurrentHashMap()

        override fun queueMetrics(queue: Queue<*>): QueueMetrics {
            return queues.computeIfAbsent(queue.name) { PrometheusQueueMetrics(queue, this) }
        }

        /**
         * Builder for flexible [Config] creation, when only some of the properties need to be set.
         */
        class Builder internal constructor() {
            private var preciseStringSize: Boolean = false
            private var registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry

            /**
             * Sets [Config.preciseStringSize] – whether message sizes are measured in bytes instead of characters.
             */
            fun preciseStringSize(preciseStringSize: Boolean) = apply { this.preciseStringSize = preciseStringSize }

            /**
             * Sets [Config.registry] – the Prometheus registry all metrics are registered in.
             */
            fun registry(registry: PrometheusRegistry) = apply { this.registry = registry }

            /**
             * Creates a new [Config] instance, a fresh one on every call.
             *
             * The metrics themselves are registered once per registry and shared by all its [Config] objects – see [Config].
             */
            fun build() = Config(preciseStringSize, registry)
        }

        companion object {
            @JvmStatic
            fun builder(): Builder = Builder()
        }
    }
}

