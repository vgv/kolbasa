package kolbasa.stats.opentelemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor
import kolbasa.Kolbasa
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult

internal class QueueTracing<Data, Meta : Any>(queueName: String) {

    private val instrumenter: Instrumenter<SendRequest<Data, Meta>, SendResult<Data, Meta>> =
        buildProducerInstrumenter(Kolbasa.openTelemetryConfig.openTelemetry, queueName)

    internal fun makeCall(
        request: SendRequest<Data, Meta>,
        businessCall: () -> SendResult<Data, Meta>
    ): SendResult<Data, Meta> {
        if (!Kolbasa.openTelemetryConfig.enabled) {
            return businessCall()
        }

        val parentContext = Context.current()
        if (!instrumenter.shouldStart(parentContext, request)) {
            return businessCall()
        }

        // Start instrumenting
        val context = instrumenter.start(parentContext, request)
        val response = try {
            context.makeCurrent().use {
                businessCall()
            }
        } catch (e: Exception) {
            instrumenter.end(context, request, null, e)
            throw e
        }
        instrumenter.end(context, request, response, null)

        return response
    }

    private fun buildProducerInstrumenter(
        openTelemetry: OpenTelemetry,
        queueName: String
    ): Instrumenter<SendRequest<Data, Meta>, SendResult<Data, Meta>> {
        val sendRequestAttributesGetter = SendRequestAttributesGetter<Data, Meta>(queueName)

        val spanNameExtractor = MessagingSpanNameExtractor
            .create(sendRequestAttributesGetter, MessageOperation.PUBLISH)

        val attributesExtractor = MessagingAttributesExtractor
            .builder(sendRequestAttributesGetter, MessageOperation.PUBLISH)
            .build()

        return Instrumenter
            .builder<SendRequest<Data, Meta>, SendResult<Data, Meta>>(openTelemetry, "kolbasa", spanNameExtractor)
            .addAttributesExtractor(attributesExtractor)
            .addAttributesExtractor(SendRequestAttributesExtractor())
            .buildProducerInstrumenter(ContextToMessageSetter())
    }


}
