package kolbasa.stats.prometheus

import io.prometheus.metrics.model.registry.PrometheusRegistry

data class PrometheusConfig(
    val enabled: Boolean = false,
    val registry: PrometheusRegistry = PrometheusRegistry.defaultRegistry
)
