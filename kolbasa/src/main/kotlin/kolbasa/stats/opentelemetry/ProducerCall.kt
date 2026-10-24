package kolbasa.stats.opentelemetry

import kolbasa.producer.SendRequest
import kolbasa.schema.NodeId

internal class ProducerCall(
    val queueName: String,
    val nodeId: NodeId,
    val request: SendRequest<*>
) {
    // Trace context of the producer span, as [key1, value1, key2, value2...]
    val openTelemetryContext = mutableListOf<String>()
}
