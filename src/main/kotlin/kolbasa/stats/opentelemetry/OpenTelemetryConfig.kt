package kolbasa.stats.opentelemetry

import io.opentelemetry.api.OpenTelemetry

/**
 * Says whether Kolbasa reports traces to OpenTelemetry, and where.
 *
 * There are two states, and nothing in between: [None] collects and propagates nothing, [Config] does the work with
 * the [OpenTelemetry] instance you give it. [None] is the default, so tracing costs you nothing until you ask for it.
 *
 * When tracing is on, `send()` and `receive()` become spans, and the trace context of the sender is written into
 * the message itself. The receiving application reads it back and links its span to the span that sent the message,
 * so both sides of the queue can be seen as one flow even though they are different applications and different
 * moments in time.
 *
 * Set it through [Kolbasa.openTelemetryConfig][kolbasa.Kolbasa.openTelemetryConfig] at application startup, before
 * you create your queues and start sending. Any later changes will have no effect.
 *
 * ## Usage Example
 *
 * ```kotlin
 * Kolbasa.openTelemetryConfig = OpenTelemetryConfig.Config(openTelemetry)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setOpenTelemetryConfig(new OpenTelemetryConfig.Config(openTelemetry));
 * ```
 */
sealed class OpenTelemetryConfig {

    /** No tracing at all: nothing is measured, nothing is written into messages, nothing is read back. */
    object None : OpenTelemetryConfig()

    /**
     * Tracing through the [OpenTelemetry] instance you provide.
     */
    data class Config(
        /**
         * OpenTelemetry instance. By default, [OpenTelemetry.noop] is used.
         */
        val openTelemetry: OpenTelemetry = OpenTelemetry.noop()
    ) : OpenTelemetryConfig()

}
