package kolbasa.stats.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter

internal class ConsumerResponseAttributesGetter<Data>(private val queueName: String) :
    MessagingAttributesGetter<ConsumerCall<Data>, Unit> {

    override fun getSystem(request: ConsumerCall<Data>?): String {
        return "kolbasa"
    }

    override fun getDestination(request: ConsumerCall<Data>?): String {
        return queueName
    }

    override fun isTemporaryDestination(request: ConsumerCall<Data>?): Boolean {
        return false
    }

    override fun getConversationId(request: ConsumerCall<Data>?): String? {
        return null
    }

    override fun getMessageId(request: ConsumerCall<Data>, response: Unit?): String? {
        return if (request.messages.size == 1) {
            request.messages[0].id.toString()
        } else {
            null
        }
    }

    override fun getMessageHeader(request: ConsumerCall<Data>?, name: String?): List<String> {
        if (request == null || name == null) {
            return emptyList()
        }

        return request.messages.mapNotNull { it.openTelemetryData?.get(name) }
    }

    override fun getDestinationTemplate(request: ConsumerCall<Data>?): String? {
        return null
    }

    override fun isAnonymousDestination(request: ConsumerCall<Data>?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: ConsumerCall<Data>?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: ConsumerCall<Data>?): Long? {
        return null
    }

    override fun getClientId(request: ConsumerCall<Data>?): String? {
        return null
    }

    override fun getBatchMessageCount(request: ConsumerCall<Data>, response: Unit?): Long? {
        return if (request.messages.size > 1) {
            request.messages.size.toLong()
        } else {
            null
        }
    }
}
