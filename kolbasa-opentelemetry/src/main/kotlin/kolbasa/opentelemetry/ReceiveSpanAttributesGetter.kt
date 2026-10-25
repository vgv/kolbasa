package kolbasa.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter

internal class ReceiveSpanAttributesGetter : MessagingAttributesGetter<ReceiveSpanInput, Unit> {

    override fun getSystem(request: ReceiveSpanInput?): String {
        return "kolbasa"
    }

    override fun getDestination(request: ReceiveSpanInput?): String? {
        return request?.queueName
    }

    override fun isTemporaryDestination(request: ReceiveSpanInput?): Boolean {
        return false
    }

    override fun getConversationId(request: ReceiveSpanInput?): String? {
        return null
    }

    override fun getMessageId(request: ReceiveSpanInput, response: Unit?): String? {
        return if (request.messages.size == 1) {
            request.messages[0].id.toString()
        } else {
            null
        }
    }

    override fun getDestinationTemplate(request: ReceiveSpanInput?): String? {
        return null
    }

    override fun isAnonymousDestination(request: ReceiveSpanInput?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: ReceiveSpanInput?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: ReceiveSpanInput?): Long? {
        return null
    }

    override fun getClientId(request: ReceiveSpanInput?): String? {
        return null
    }

    override fun getBatchMessageCount(request: ReceiveSpanInput, response: Unit?): Long? {
        return if (request.messages.size > 1) {
            request.messages.size.toLong()
        } else {
            null
        }
    }
}
