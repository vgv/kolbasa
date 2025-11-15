package kolbasa.stats.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter
import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult

internal class SendRequestAttributesGetter<Data>(private val queueName: String) :
    MessagingAttributesGetter<SendRequest<Data>, SendResult<Data>> {

    override fun getSystem(request: SendRequest<Data>?): String {
        return "kolbasa"
    }

    override fun getDestination(request: SendRequest<Data>?): String {
        return queueName
    }

    override fun isTemporaryDestination(request: SendRequest<Data>?): Boolean {
        return false
    }

    override fun getConversationId(request: SendRequest<Data>?): String? {
        return null
    }

    override fun getMessageId(request: SendRequest<Data>?, response: SendResult<Data>?): String? {
        return null
    }

    override fun getMessageHeader(request: SendRequest<Data>?, name: String?): MutableList<String> {
        return super.getMessageHeader(request, name)
    }

    override fun getDestinationTemplate(request: SendRequest<Data>?): String? {
        return null
    }

    override fun isAnonymousDestination(request: SendRequest<Data>?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: SendRequest<Data>?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: SendRequest<Data>?): Long? {
        return null
    }

    override fun getClientId(request: SendRequest<Data>?): String? {
        return null
    }

    override fun getBatchMessageCount(request: SendRequest<Data>, response: SendResult<Data>?): Long? {
        return if (request.data.size > 1) {
            request.data.size.toLong()
        } else {
            null
        }
    }
}

internal class ConsumerResponseAttributesGetter<Data>(private val queueName: String) :
    MessagingAttributesGetter<List<Message<Data>>, Unit> {

    override fun getSystem(request: List<Message<Data>>?): String {
        return "kolbasa"
    }

    override fun getDestination(request: List<Message<Data>>?): String {
        return queueName
    }

    override fun isTemporaryDestination(request: List<Message<Data>>?): Boolean {
        return false
    }

    override fun getConversationId(request: List<Message<Data>>?): String? {
        return null
    }

    override fun getMessageId(request: List<Message<Data>>, response: Unit?): String? {
        return if (request.size == 1) {
            request[0].id.toString()
        } else {
            null
        }
    }

    override fun getMessageHeader(request: List<Message<Data>>?, name: String?): List<String> {
        if (request == null || name == null) {
            return emptyList()
        }

        return request.mapNotNull { it.openTelemetryData?.get(name) }
    }

    override fun getDestinationTemplate(request: List<Message<Data>>?): String? {
        return null
    }

    override fun isAnonymousDestination(request: List<Message<Data>>?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: List<Message<Data>>?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: List<Message<Data>>?): Long? {
        return null
    }

    override fun getClientId(request: List<Message<Data>>?): String? {
        return null
    }

    override fun getBatchMessageCount(request: List<Message<Data>>, response: Unit?): Long? {
        return if (request.size > 1) {
            return request.size.toLong()
        } else {
            null
        }
    }
}
