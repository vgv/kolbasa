package kolbasa.stats.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter

internal class ConsumerResponseAttributesGetter : MessagingAttributesGetter<ConsumerCall, Unit> {

    override fun getSystem(request: ConsumerCall?): String {
        return "kolbasa"
    }

    override fun getDestination(request: ConsumerCall?): String? {
        return request?.queueName
    }

    override fun isTemporaryDestination(request: ConsumerCall?): Boolean {
        return false
    }

    override fun getConversationId(request: ConsumerCall?): String? {
        return null
    }

    override fun getMessageId(request: ConsumerCall, response: Unit?): String? {
        return if (request.messages.size == 1) {
            request.messages[0].id.toString()
        } else {
            null
        }
    }

    override fun getMessageHeader(request: ConsumerCall?, name: String?): List<String> {
        if (request == null || name == null) {
            return emptyList()
        }

        return request.messages.mapNotNull { it.openTelemetryData?.get(name) }
    }

    override fun getDestinationTemplate(request: ConsumerCall?): String? {
        return null
    }

    override fun isAnonymousDestination(request: ConsumerCall?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: ConsumerCall?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: ConsumerCall?): Long? {
        return null
    }

    override fun getClientId(request: ConsumerCall?): String? {
        return null
    }

    override fun getBatchMessageCount(request: ConsumerCall, response: Unit?): Long? {
        return if (request.messages.size > 1) {
            request.messages.size.toLong()
        } else {
            null
        }
    }
}
