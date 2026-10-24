package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.schema.NodeId

internal class ReceiveSpanInput(
    val queueName: String,
    val nodeId: NodeId,
    val messages: List<Message<*>>,
    // Trace contexts of the messages, in the same order
    val traceContexts: List<Map<String, String>>
)
