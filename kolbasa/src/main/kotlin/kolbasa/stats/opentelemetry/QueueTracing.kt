package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.schema.NodeId

internal interface QueueTracing<Data> {

    fun readOpenTelemetryData(): Boolean

    fun makeProducerCall(nodeId: NodeId, request: SendRequest<Data>, businessCall: () -> SendResult<Data>): SendResult<Data>

    fun makeConsumerCall(nodeId: NodeId, businessCall: () -> List<Message<Data>>): List<Message<Data>>

}

