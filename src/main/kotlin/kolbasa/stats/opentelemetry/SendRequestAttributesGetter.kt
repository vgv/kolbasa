package kolbasa.stats.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter
import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult

internal class SendRequestAttributesGetter<Data, Meta : Any>(private val queueName: String) :
    MessagingAttributesGetter<SendRequest<Data, Meta>, SendResult<Data, Meta>> {

    override fun getSystem(request: SendRequest<Data, Meta>?): String {
        return "kolbasa"
    }

    override fun getDestination(request: SendRequest<Data, Meta>?): String {
        return queueName
    }

    override fun isTemporaryDestination(request: SendRequest<Data, Meta>?): Boolean {
        return false
    }

    override fun getConversationId(request: SendRequest<Data, Meta>?): String? {
        return null
    }

    override fun getMessageId(request: SendRequest<Data, Meta>?, response: SendResult<Data, Meta>?): String? {
        return null
    }

    override fun getMessageHeader(request: SendRequest<Data, Meta>?, name: String?): MutableList<String> {
        return super.getMessageHeader(request, name)
    }

    override fun getDestinationTemplate(request: SendRequest<Data, Meta>?): String? {
        return null
    }

    override fun isAnonymousDestination(request: SendRequest<Data, Meta>?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: SendRequest<Data, Meta>?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: SendRequest<Data, Meta>?): Long? {
        return null
    }

    override fun getClientId(request: SendRequest<Data, Meta>?): String? {
        return null
    }

    override fun getBatchMessageCount(request: SendRequest<Data, Meta>, response: SendResult<Data, Meta>?): Long? {
        return if (request.data.size > 1) {
            request.data.size.toLong()
        } else {
            null
        }
    }
}

internal class ConsumerResponseAttributesGetter<Data, Meta : Any>(private val queueName: String) :
    MessagingAttributesGetter<List<Message<Data, Meta>>, Unit> {

    override fun getSystem(request: List<Message<Data, Meta>>?): String {
        return "kolbasa"
    }

    override fun getDestination(request: List<Message<Data, Meta>>?): String {
        return queueName
    }

    override fun isTemporaryDestination(request: List<Message<Data, Meta>>?): Boolean {
        return false
    }

    override fun getConversationId(request: List<Message<Data, Meta>>?): String? {
        return null
    }

    override fun getMessageId(request: List<Message<Data, Meta>>, response: Unit?): String? {
        return if (request.size == 1) {
            request[0].id.toString()
        } else {
            null
        }
    }

    override fun getMessageHeader(request: List<Message<Data, Meta>>?, name: String?): List<String> {
        if (request == null || name == null) {
            return emptyList()
        }

        return request.mapNotNull { it.openTelemetryData?.get(name) }
    }

    override fun getDestinationTemplate(request: List<Message<Data, Meta>>?): String? {
        return null
    }

    override fun isAnonymousDestination(request: List<Message<Data, Meta>>?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: List<Message<Data, Meta>>?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: List<Message<Data, Meta>>?): Long? {
        return null
    }

    override fun getClientId(request: List<Message<Data, Meta>>?): String? {
        return null
    }

    override fun getBatchMessageCount(request: List<Message<Data, Meta>>, response: Unit?): Long? {
        return if (request.size > 1) {
            return request.size.toLong()
        } else {
            null
        }
    }
}
