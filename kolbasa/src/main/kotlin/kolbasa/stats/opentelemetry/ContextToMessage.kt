package kolbasa.stats.opentelemetry

import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapSetter
import kolbasa.consumer.Message

internal class ContextToMessageSetter<Data> : TextMapSetter<ProducerCall<Data>> {

    override fun set(carrier: ProducerCall<Data>?, key: String, value: String) {
        if (carrier == null) {
            return
        }

        carrier.request.addOpenTelemetryContext(key, value)
    }
}

internal class ContextFromMessageGetter<Data> : TextMapGetter<Message<Data>> {

    override fun keys(carrier: Message<Data>): Iterable<String> {
        return carrier.openTelemetryData?.keys ?: emptyList()
    }

    override fun get(carrier: Message<Data>?, key: String): String? {
        return carrier?.openTelemetryData?.get(key)
    }
}
