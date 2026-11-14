package kolbasa.spring.boot

import io.opentelemetry.context.Context
import io.opentelemetry.sdk.trace.ReadWriteSpan
import io.opentelemetry.sdk.trace.ReadableSpan
import io.opentelemetry.sdk.trace.SpanProcessor
import java.util.concurrent.CopyOnWriteArrayList

// Remembers the names of the spans that ended, in that order
internal class RecordingSpanProcessor : SpanProcessor {

    val ended = CopyOnWriteArrayList<String>()

    override fun onStart(parentContext: Context, span: ReadWriteSpan) {}

    override fun isStartRequired() = false

    override fun onEnd(span: ReadableSpan) {
        ended += span.name
    }

    override fun isEndRequired() = true
}
