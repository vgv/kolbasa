package kolbasa.stats.opentelemetry

import io.opentelemetry.api.OpenTelemetry

sealed class OpenTelemetryConfig {

    object None : OpenTelemetryConfig()

    data class Config(
        /**
         * OpenTelemetry instance. By default, [OpenTelemetry.noop] is used.
         */
        val openTelemetry: OpenTelemetry = OpenTelemetry.noop()
    ) : OpenTelemetryConfig()

}
