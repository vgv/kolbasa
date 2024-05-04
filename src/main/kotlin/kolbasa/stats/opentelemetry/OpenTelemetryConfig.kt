package kolbasa.stats.opentelemetry

import io.opentelemetry.api.OpenTelemetry

data class OpenTelemetryConfig(
    /**
     * Collect OpenTelemetry traces or not
     */
    val enabled: Boolean = false,
    /**
     * OpenTelemetry instance. By default, [OpenTelemetry.noop] is used.
     */
    val openTelemetry: OpenTelemetry = OpenTelemetry.noop()
)
