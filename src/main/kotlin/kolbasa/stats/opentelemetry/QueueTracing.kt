package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult

internal interface QueueTracing<Data> {

    fun readOpenTelemetryData(): Boolean

    fun makeProducerCall(request: SendRequest<Data>, businessCall: () -> SendResult<Data>): SendResult<Data>

    fun makeConsumerCall(businessCall: () -> List<Message<Data>>): List<Message<Data>>

}

