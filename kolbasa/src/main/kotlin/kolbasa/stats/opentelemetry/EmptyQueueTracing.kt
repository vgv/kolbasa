package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.schema.NodeId

internal class EmptyQueueTracing<Data>: QueueTracing<Data> {

    override fun readOpenTelemetryData(): Boolean {
        return false
    }

    override fun makeProducerCall(
        nodeId: NodeId,
        request: SendRequest<Data>,
        businessCall: () -> SendResult<Data>
    ): SendResult<Data> {
        // default trivial implementation
        return businessCall()
    }

    override fun makeConsumerCall(nodeId: NodeId, businessCall: () -> List<Message<Data>>): List<Message<Data>> {
        // default trivial implementation
        return businessCall()
    }

}
