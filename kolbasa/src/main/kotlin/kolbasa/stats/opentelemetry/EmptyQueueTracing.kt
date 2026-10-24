package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.schema.NodeId

internal object EmptyQueueTracing : QueueTracing {

    override fun <Data> makeProducerCall(
        queueName: String,
        nodeId: NodeId,
        request: SendRequest<Data>,
        businessCall: (openTelemetryContext: List<String>?) -> SendResult<Data>
    ): SendResult<Data> {
        // default trivial implementation
        return businessCall(null)
    }

    override fun <Data> makeConsumerCall(
        queueName: String,
        nodeId: NodeId,
        businessCall: () -> List<Message<Data>>
    ): List<Message<Data>> {
        // default trivial implementation
        return businessCall()
    }

}
