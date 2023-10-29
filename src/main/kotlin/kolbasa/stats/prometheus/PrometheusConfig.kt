package kolbasa.stats.prometheus

import io.prometheus.metrics.model.registry.PrometheusRegistry

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
     * Prometheus registry. By default, [PrometheusRegistry.defaultRegistry] is used.
     * You can use your custom registry, but you need to change the registry before any
     * other queue operations (at application startup, for example).
     */
    val registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry
)
