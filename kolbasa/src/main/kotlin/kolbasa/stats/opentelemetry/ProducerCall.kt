package kolbasa.stats.opentelemetry

import kolbasa.producer.SendRequest
import kolbasa.schema.NodeId

internal class ProducerCall(
    val queueName: String,
    val nodeId: NodeId,
    val request: SendRequest<*>
) {
    // Trace context of the producer span
    val traceContext = mutableMapOf<String, String>()
}
