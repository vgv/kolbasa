package kolbasa.opentelemetry

import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapSetter

internal class TraceContextSetter : TextMapSetter<MutableMap<String, String>> {

    override fun set(carrier: MutableMap<String, String>?, key: String, value: String) {
        if (carrier == null) {
            return
        }

        carrier[key] = value
    }
}

internal class TraceContextGetter : TextMapGetter<Map<String, String>> {

    override fun keys(carrier: Map<String, String>): Iterable<String> {
        return carrier.keys
    }

    override fun get(carrier: Map<String, String>?, key: String): String? {
        return carrier?.get(key)
    }
}

// The same for one processed message: OpenTelemetry reads the sender's context from it and makes it the parent
internal class ProcessTraceContextGetter : TextMapGetter<ProcessSpanInput> {

    override fun keys(carrier: ProcessSpanInput): Iterable<String> {
        return carrier.traceContext.keys
    }

    override fun get(carrier: ProcessSpanInput?, key: String): String? {
        return carrier?.traceContext?.get(key)
    }
}
