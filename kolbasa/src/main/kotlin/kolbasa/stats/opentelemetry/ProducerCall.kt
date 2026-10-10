package kolbasa.stats.opentelemetry

import kolbasa.producer.SendRequest
import kolbasa.schema.NodeId

internal class ProducerCall<Data>(
    val request: SendRequest<Data>,
    val nodeId: NodeId
)
