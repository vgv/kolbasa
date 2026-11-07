package kolbasa.telemetry.prometheus

import io.prometheus.metrics.model.registry.PrometheusRegistry
import kolbasa.queue.Queue
import kolbasa.telemetry.prometheus.metrics.PrometheusMetrics

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
     * You can build as many [Config] objects over one registry as you like: they share the same metrics, and a new one
     * costs almost nothing. There is no need to pass one object around.
     */
    data class Config(
        /**
         * Prometheus registry. By default, [PrometheusRegistry.defaultRegistry] is used.
         */
        val registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry
    ) : PrometheusConfig() {

        // All metrics of the library, shared by every Config over the same registry
        private val metrics: PrometheusMetrics = PrometheusMetrics.of(registry)

        override fun queueMetrics(queue: Queue<*>): QueueMetrics {
            return metrics.queueMetrics(queue)
        }
    }
}

