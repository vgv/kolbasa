package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.schema.NodeId

internal class ConsumerCall<Data>(
    val messages: List<Message<Data>>,
    val nodeId: NodeId
)
