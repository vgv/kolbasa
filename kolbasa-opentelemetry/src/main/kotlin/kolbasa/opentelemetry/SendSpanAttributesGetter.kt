package kolbasa.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter
import kolbasa.producer.SendResult

internal class SendSpanAttributesGetter : MessagingAttributesGetter<SendSpanInput, SendResult<*>> {

    override fun getSystem(request: SendSpanInput?): String {
        return "kolbasa"
    }

    override fun getDestination(request: SendSpanInput?): String? {
        return request?.queueName
    }

    override fun isTemporaryDestination(request: SendSpanInput?): Boolean {
        return false
    }

    override fun getConversationId(request: SendSpanInput?): String? {
        return null
    }

    override fun getMessageId(request: SendSpanInput?, response: SendResult<*>?): String? {
        return null
    }

    override fun getDestinationTemplate(request: SendSpanInput?): String? {
        return null
    }

    override fun isAnonymousDestination(request: SendSpanInput?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: SendSpanInput?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: SendSpanInput?): Long? {
        return null
    }

    override fun getClientId(request: SendSpanInput?): String? {
        return null
    }

    override fun getBatchMessageCount(request: SendSpanInput, response: SendResult<*>?): Long? {
        return if (request.request.data.size > 1) {
            request.request.data.size.toLong()
        } else {
            null
        }
    }
}
