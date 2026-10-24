package kolbasa.opentelemetry

import kolbasa.producer.SendRequest
import kolbasa.schema.NodeId

internal class SendSpanInput(
    val queueName: String,
    val nodeId: NodeId,
    val request: SendRequest<*>
) {
    // Trace context of the producer span
    val traceContext = mutableMapOf<String, String>()
}
