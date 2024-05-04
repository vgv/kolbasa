package kolbasa.stats.opentelemetry

import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapSetter
import kolbasa.consumer.Message
import kolbasa.producer.SendRequest

internal class ContextToMessageSetter<Data, Meta : Any> : TextMapSetter<SendRequest<Data, Meta>> {

    override fun set(carrier: SendRequest<Data, Meta>?, key: String, value: String) {
        if (carrier == null) {
            return
        }

        carrier.addOpenTelemetryContext(key, value)
    }
}

internal class ContextFromMessageGetter<Data, Meta : Any> : TextMapGetter<Message<Data, Meta>> {

    override fun keys(carrier: Message<Data, Meta>): Iterable<String> {
        return carrier.openTelemetryData?.keys ?: emptyList()
    }

    override fun get(carrier: Message<Data, Meta>?, key: String): String? {
        return carrier?.openTelemetryData?.get(key)
    }
}
