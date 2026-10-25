package kolbasa.opentelemetry

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter
import kolbasa.consumer.worker.HandleResult

internal class ProcessSpanAttributesGetter : MessagingAttributesGetter<ProcessSpanInput, HandleResult> {

    override fun getSystem(request: ProcessSpanInput?): String {
        return "kolbasa"
    }

    override fun getDestination(request: ProcessSpanInput?): String? {
        return request?.queueName
    }

    override fun isTemporaryDestination(request: ProcessSpanInput?): Boolean {
        return false
    }

    override fun getConversationId(request: ProcessSpanInput?): String? {
        return null
    }

    override fun getMessageId(request: ProcessSpanInput, response: HandleResult?): String? {
        return request.message.id.toString()
    }

    override fun getDestinationTemplate(request: ProcessSpanInput?): String? {
        return null
    }

    override fun isAnonymousDestination(request: ProcessSpanInput?): Boolean {
        return false
    }

    override fun getMessageBodySize(request: ProcessSpanInput?): Long? {
        return null
    }

    override fun getMessageEnvelopeSize(request: ProcessSpanInput?): Long? {
        return null
    }

    override fun getClientId(request: ProcessSpanInput?): String? {
        return null
    }

    override fun getBatchMessageCount(request: ProcessSpanInput, response: HandleResult?): Long? {
        return null
    }
}
