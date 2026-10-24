package kolbasa.stats.opentelemetry

import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapSetter
import kolbasa.consumer.Message

internal class ContextToMessageSetter : TextMapSetter<ProducerCall> {

    override fun set(carrier: ProducerCall?, key: String, value: String) {
        if (carrier == null) {
            return
        }

        carrier.openTelemetryContext += key
        carrier.openTelemetryContext += value
    }
}

internal class ContextFromMessageGetter : TextMapGetter<Message<*>> {

    override fun keys(carrier: Message<*>): Iterable<String> {
        return carrier.openTelemetryData?.keys ?: emptyList()
    }

    override fun get(carrier: Message<*>?, key: String): String? {
        return carrier?.openTelemetryData?.get(key)
    }
}
