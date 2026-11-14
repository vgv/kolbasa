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
