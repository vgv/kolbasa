package kolbasa.opentelemetry

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor

internal class ProcessSpanLinksExtractor : SpanLinksExtractor<ProcessSpanInput> {

    override fun extract(spanLinks: SpanLinksBuilder, parentContext: Context, request: ProcessSpanInput) {
        // Three spans matter here:
        //   process span  - the one being made now: the handler working on this message
        //   sender's span - the span that sent the message. OpenTelemetry has made it the parent already
        //   caller's span - the span that was current when QueueProcessor.process() was called, e.g. a scheduled job
        //
        // So we add a link from the process span to the caller's span. Then, looking at the processing of a message,
        // you can find the job that did it:
        //   the message has a context  -> parent: sender's span,  link: caller's span
        //   the message has no context -> parent: caller's span,  no link, it is the parent already
        //     (sent without tracing, or from SQL by the q_<queue>_put() function)
        //   there is no caller's span  -> parent: sender's span,  no link (e.g. in a QueueWorker thread)
        val caller = request.callerSpanContext
        if (caller.isValid && caller != Span.fromContext(parentContext).spanContext) {
            spanLinks.addLink(caller)
        }
    }
}
