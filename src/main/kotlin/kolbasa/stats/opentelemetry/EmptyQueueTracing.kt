package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult

internal class EmptyQueueTracing<Data>: QueueTracing<Data> {

    override fun readOpenTelemetryData(): Boolean {
        return false
    }

    override fun makeProducerCall(
        request: SendRequest<Data>,
        businessCall: () -> SendResult<Data>
    ): SendResult<Data> {
        // default trivial implementation
        return businessCall()
    }

    override fun makeConsumerCall(businessCall: () -> List<Message<Data>>): List<Message<Data>> {
        // default trivial implementation
        return businessCall()
    }

}
