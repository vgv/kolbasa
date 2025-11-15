package kolbasa.stats.opentelemetry

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor
import kolbasa.consumer.Message

internal class ConsumerSpanLinksExtractor<Data>(
    private val propagator: TextMapPropagator,
    private val extractor: TextMapGetter<Message<Data>>
) : SpanLinksExtractor<List<Message<Data>>> {

    override fun extract(spanLinks: SpanLinksBuilder, parentContext: Context, request: List<Message<Data>>) {
        // Extract all context information from all messages and add them as links
        request.forEach { req ->
            val context = propagator.extract(Context.root(), req, extractor)
            spanLinks.addLink(Span.fromContext(context).spanContext)
        }
    }
}
