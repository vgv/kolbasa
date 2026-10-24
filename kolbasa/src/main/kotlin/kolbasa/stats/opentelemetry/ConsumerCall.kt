package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.schema.NodeId

internal class ConsumerCall(
    val queueName: String,
    val nodeId: NodeId,
    val messages: List<Message<*>>
)
