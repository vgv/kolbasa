package kolbasa.stats.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter
import kolbasa.producer.SendResult

internal class SendRequestAttributesGetter<Data>(private val queueName: String) :
    MessagingAttributesGetter<ProducerCall<Data>, SendResult<Data>> {

    override fun getSystem(request: ProducerCall<Data>?): String {
        return "kolbasa"
    }

    override fun getDestination(request: ProducerCall<Data>?): String {
        return queueName
    }

    override fun isTemporaryDestination(request: ProducerCall<Data>?): Boolean {
        return false
    }

    override fun getConversationId(request: ProducerCall<Data>?): String? {
        return null
    }

    override fun getMessageId(request: ProducerCall<Data>?, response: SendResult<Data>?): String? {
        return null
    }

    override fun getMessageHeader(request: ProducerCall<Data>?, name: String?): MutableList<String> {
        return super.getMessageHeader(request, name)
    }

    override fun getDestinationTemplate(request: ProducerCall<Data>?): String? {
        return null
    }

    override fun isAnonymousDestination(request: ProducerCall<Data>?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: ProducerCall<Data>?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: ProducerCall<Data>?): Long? {
        return null
    }

    override fun getClientId(request: ProducerCall<Data>?): String? {
        return null
    }

    override fun getBatchMessageCount(request: ProducerCall<Data>, response: SendResult<Data>?): Long? {
        return if (request.request.data.size > 1) {
            request.request.data.size.toLong()
        } else {
            null
        }
    }
}
