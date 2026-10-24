package kolbasa.stats.opentelemetry

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor
import kolbasa.consumer.Message

internal class ConsumerSpanLinksExtractor(
    private val propagator: TextMapPropagator,
    private val extractor: TextMapGetter<Message<*>>
) : SpanLinksExtractor<ConsumerCall> {

    override fun extract(spanLinks: SpanLinksBuilder, parentContext: Context, request: ConsumerCall) {
        val root = Context.root()

        // Extract all context information from all messages and add them as links
        request.messages.forEach { req ->
            val context = propagator.extract(root, req, extractor)
            spanLinks.addLink(Span.fromContext(context).spanContext)
        }
    }
}
