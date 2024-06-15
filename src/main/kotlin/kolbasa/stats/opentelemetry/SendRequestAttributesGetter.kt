package kolbasa.stats.opentelemetry

import io.opentelemetry.api.common.AttributesBuilder
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor
import io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes
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
        return null
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
        return null
    }
}


internal class SendRequestAttributesExtractor<Data, Meta : Any> : AttributesExtractor<SendRequest<Data, Meta>, SendResult<Data, Meta>> {

    override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: SendRequest<Data, Meta>) {
        if (request.data.size > 1) {
            attributes.put(MessagingIncubatingAttributes.MESSAGING_BATCH_MESSAGE_COUNT, request.data.size.toLong())
        }
    }

    override fun onEnd(
        attributes: AttributesBuilder,
        context: Context,
        request: SendRequest<Data, Meta>,
        response: SendResult<Data, Meta>?,
        error: Throwable?
    ) {
        // NOP
    }
}

internal class ConsumerResponseAttributesExtractor<Data, Meta: Any>: AttributesExtractor<List<Message<Data, Meta>>, Unit> {

    override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: List<Message<Data, Meta>>) {
        if (request.size > 1) {
            attributes.put(MessagingIncubatingAttributes.MESSAGING_BATCH_MESSAGE_COUNT, request.size.toLong())
        }
    }

    override fun onEnd(
        attributes: AttributesBuilder,
        context: Context,
        request: List<Message<Data, Meta>>,
        response: Unit?,
        error: Throwable?
    ) {
        // NOP
    }
}

