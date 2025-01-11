package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult

internal class EmptyQueueTracing<Data, Meta : Any>: QueueTracing<Data, Meta> {

    override fun readOpenTelemetryData(): Boolean {
        return false
    }

    override fun makeProducerCall(
        request: SendRequest<Data, Meta>,
        businessCall: () -> SendResult<Data, Meta>
    ): SendResult<Data, Meta> {
        // default trivial implementation
        return businessCall()
    }

    override fun makeConsumerCall(businessCall: () -> List<Message<Data, Meta>>): List<Message<Data, Meta>> {
        // default trivial implementation
        return businessCall()
    }

}
