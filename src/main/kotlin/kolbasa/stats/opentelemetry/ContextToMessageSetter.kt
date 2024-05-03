package kolbasa.stats.opentelemetry

import io.opentelemetry.context.propagation.TextMapSetter
import kolbasa.producer.SendRequest

internal class ContextToMessageSetter<Data, Meta : Any> : TextMapSetter<SendRequest<Data, Meta>> {

    override fun set(carrier: SendRequest<Data, Meta>?, key: String, value: String) {
        if (carrier == null) {
            return
        }

        carrier.addOpenTelemetryContext(key, value)
    }
}
