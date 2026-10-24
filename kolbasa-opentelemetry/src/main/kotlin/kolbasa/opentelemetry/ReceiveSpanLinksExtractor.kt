package kolbasa.opentelemetry

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor

internal class ReceiveSpanLinksExtractor(
    private val propagator: TextMapPropagator,
    private val extractor: TextMapGetter<Map<String, String>>
) : SpanLinksExtractor<ReceiveSpanInput> {

    override fun extract(spanLinks: SpanLinksBuilder, parentContext: Context, request: ReceiveSpanInput) {
        val root = Context.root()

        // Extract all context information from all messages and add them as links
        request.traceContexts.forEach { traceContext ->
            val context = propagator.extract(root, traceContext, extractor)
            spanLinks.addLink(Span.fromContext(context).spanContext)
        }
    }
}
