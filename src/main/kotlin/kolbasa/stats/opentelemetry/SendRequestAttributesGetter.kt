package kolbasa.stats.opentelemetry

import io.opentelemetry.api.common.AttributesBuilder
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter
import io.opentelemetry.semconv.SemanticAttributes
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

    override fun getMessagePayloadSize(request: SendRequest<Data, Meta>?): Long? {
        return null
    }

    override fun getMessagePayloadCompressedSize(request: SendRequest<Data, Meta>?): Long? {
        return null
    }

    override fun getMessageId(request: SendRequest<Data, Meta>?, response: SendResult<Data, Meta>?): String? {
        return null
    }

    override fun getMessageHeader(request: SendRequest<Data, Meta>?, name: String?): MutableList<String> {
        return super.getMessageHeader(request, name)
    }
}

internal class SendRequestAttributesExtractor<Data, Meta : Any> : AttributesExtractor<SendRequest<Data, Meta>, SendResult<Data, Meta>> {

    override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: SendRequest<Data, Meta>) {
        if (request.data.size > 1) {
            attributes.put(SemanticAttributes.MESSAGING_BATCH_MESSAGE_COUNT, request.data.size.toLong())
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
