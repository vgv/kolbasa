package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.schema.NodeId

internal interface QueueTracing<Data> {

    fun readOpenTelemetryData(): Boolean

    // businessCall gets the trace context to write into the sent messages, as [key1, value1, key2, value2...],
    // or null if there is nothing to write
    fun makeProducerCall(
        nodeId: NodeId,
        request: SendRequest<Data>,
        businessCall: (openTelemetryContext: List<String>?) -> SendResult<Data>
    ): SendResult<Data>

    fun makeConsumerCall(nodeId: NodeId, businessCall: () -> List<Message<Data>>): List<Message<Data>>

}

