package kolbasa.example

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.export.SpanExporter

object ExamplesOpenTelemetryProvider {

    /**
     * OpenTelemetry that prints every finished span to the console
     */
    @JvmStatic
    fun getOpenTelemetry(): OpenTelemetry {
        val tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(ConsoleSpanExporter))
            .build()

        // Without a propagator, the trace context is not written into messages at all
        return OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build()
    }

    // One line per span: kind, name, its own ids and the ids of the spans it links to
    private object ConsoleSpanExporter : SpanExporter {
        override fun export(spans: Collection<SpanData>): CompletableResultCode {
            spans.forEach { span ->
                val links = span.links.joinToString { "${it.spanContext.traceId}/${it.spanContext.spanId}" }
                println("${span.kind} '${span.name}' ${span.traceId}/${span.spanId}, links: [$links]")
            }
            return CompletableResultCode.ofSuccess()
        }

        override fun flush(): CompletableResultCode = CompletableResultCode.ofSuccess()

        override fun shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
    }
}
