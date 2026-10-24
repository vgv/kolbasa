package kolbasa.stats.opentelemetry

import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapSetter

internal class ContextToMessageSetter : TextMapSetter<ProducerCall> {

    override fun set(carrier: ProducerCall?, key: String, value: String) {
        if (carrier == null) {
            return
        }

        carrier.traceContext[key] = value
    }
}

internal class ContextFromMessageGetter : TextMapGetter<Map<String, String>> {

    override fun keys(carrier: Map<String, String>): Iterable<String> {
        return carrier.keys
    }

    override fun get(carrier: Map<String, String>?, key: String): String? {
        return carrier?.get(key)
    }
}
