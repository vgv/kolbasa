package kolbasa.telemetry.metrics

import io.micrometer.core.instrument.MeterRegistry
import kolbasa.queue.Queue

/**
 * Says whether Kolbasa collects metrics, and in which Micrometer registry.
 *
 * There are two states, and nothing in between: [None] collects nothing, [Config] collects everything – message
 * counters, queue sizes, message ages, call durations. [None] is the default, so metrics cost you nothing until you
 * ask for them.
 *
 * Set it for the whole application with [Kolbasa.metricsConfig][kolbasa.Kolbasa.metricsConfig], or for one
 * producer, consumer or mutator in its options, for example
 * [ProducerOptions.metricsConfig][kolbasa.producer.ProducerOptions.metricsConfig]. A change takes effect on
 * the next call.
 *
 * ## Usage Example
 *
 * ```kotlin
 * Kolbasa.metricsConfig = MetricsConfig.Config(meterRegistry)
 * // or if you don't want any metrics at all:
 * Kolbasa.metricsConfig = MetricsConfig.None
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setMetricsConfig(new MetricsConfig.Config(meterRegistry));
 * // or if you don't want any metrics at all:
 * Kolbasa.setMetricsConfig(MetricsConfig.None.INSTANCE);
 * ```
 */
sealed class MetricsConfig {

    // Metrics of one queue: what every call reports to
    internal abstract fun queueMetrics(queue: Queue<*>): QueueMetrics

    /**
     * No metrics at all: nothing is measured and nothing is registered in any registry.
     */
    object None : MetricsConfig() {
        override fun queueMetrics(queue: Queue<*>): QueueMetrics = EmptyQueueMetrics
    }

    /**
     * Metrics, registered in a Micrometer registry.
     *
     * The metrics of a queue are created and registered on the first call to that queue, not when a [Config] is
     * created. That work happens once for every queue in a registry: later calls only report values, and that costs
     * almost nothing.
     *
     * You can build as many [Config] objects over one registry as you like: they share the same metrics, and a new one
     * costs almost nothing. There is no need to pass one object around.
     */
    data class Config(
        /**
         * The registry the metrics go to: the one your application reports from, for example a
         * `PrometheusMeterRegistry`, or the `MeterRegistry` bean of Spring Boot.
         *
         * There is no default on purpose. `Metrics.globalRegistry` works too, but only once a real registry has been
         * added to it. Without one, it accepts the metrics and sends them nowhere.
         */
        val registry: MeterRegistry
    ) : MetricsConfig() {

        // All metrics of the library, shared by every Config over the same registry
        private val metrics: MicrometerMetrics = MicrometerMetrics.of(registry)

        override fun queueMetrics(queue: Queue<*>): QueueMetrics {
            return metrics.queueMetrics(queue)
        }
    }
}
