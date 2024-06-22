package kolbasa.stats.opentelemetry

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult

internal interface QueueTracing<Data, Meta : Any> {

    fun makeProducerCall(request: SendRequest<Data, Meta>, businessCall: () -> SendResult<Data, Meta>): SendResult<Data, Meta>

    fun makeConsumerCall(businessCall: () -> List<Message<Data, Meta>>): List<Message<Data, Meta>>

}

