package kolbasa.stats.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter
import kolbasa.producer.SendResult

internal class SendRequestAttributesGetter : MessagingAttributesGetter<ProducerCall, SendResult<*>> {

    override fun getSystem(request: ProducerCall?): String {
        return "kolbasa"
    }

    override fun getDestination(request: ProducerCall?): String? {
        return request?.queueName
    }

    override fun isTemporaryDestination(request: ProducerCall?): Boolean {
        return false
    }

    override fun getConversationId(request: ProducerCall?): String? {
        return null
    }

    override fun getMessageId(request: ProducerCall?, response: SendResult<*>?): String? {
        return null
    }

    override fun getMessageHeader(request: ProducerCall?, name: String?): MutableList<String> {
        return super.getMessageHeader(request, name)
    }

    override fun getDestinationTemplate(request: ProducerCall?): String? {
        return null
    }

    override fun isAnonymousDestination(request: ProducerCall?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: ProducerCall?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: ProducerCall?): Long? {
        return null
    }

    override fun getClientId(request: ProducerCall?): String? {
        return null
    }

    override fun getBatchMessageCount(request: ProducerCall, response: SendResult<*>?): Long? {
        return if (request.request.data.size > 1) {
            request.request.data.size.toLong()
        } else {
            null
        }
    }
}
