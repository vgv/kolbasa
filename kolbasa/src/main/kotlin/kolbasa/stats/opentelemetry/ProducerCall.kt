package kolbasa.stats.opentelemetry

import kolbasa.producer.SendRequest
import kolbasa.schema.NodeId

internal class ProducerCall<Data>(
    val request: SendRequest<Data>,
    val nodeId: NodeId
) {
    // Trace context of the producer span, as [key1, value1, key2, value2...]
    val openTelemetryContext = mutableListOf<String>()
}
