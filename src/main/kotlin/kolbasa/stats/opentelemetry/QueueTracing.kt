package kolbasa.stats.opentelemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessageOperation
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor
import io.opentelemetry.instrumentation.api.internal.InstrumenterUtil
import kolbasa.Kolbasa
import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import java.time.Instant

internal class QueueTracing<Data, Meta : Any>(queueName: String) {

    private val producerInstrumenter: Instrumenter<SendRequest<Data, Meta>, SendResult<Data, Meta>> =
        buildProducerInstrumenter(Kolbasa.openTelemetryConfig.openTelemetry, queueName)

    private val consumerInstrumenter: Instrumenter<List<Message<Data, Meta>>, Unit> =
        buildConsumerInstrumenter(Kolbasa.openTelemetryConfig.openTelemetry, queueName)

    internal fun makeProducerCall(
        request: SendRequest<Data, Meta>,
        businessCall: () -> SendResult<Data, Meta>
    ): SendResult<Data, Meta> {
        if (!Kolbasa.openTelemetryConfig.enabled) {
            return businessCall()
        }

        val parentContext = Context.current()
        if (!producerInstrumenter.shouldStart(parentContext, request)) {
            return businessCall()
        }

        // Start instrumenting
        val context = producerInstrumenter.start(parentContext, request)
        val response = try {
            context.makeCurrent().use {
                businessCall()
            }
        } catch (e: Exception) {
            producerInstrumenter.end(context, request, null, e)
            throw e
        }
        producerInstrumenter.end(context, request, response, null)

        return response
    }

    internal fun makeConsumerCall(businessCall: () -> List<Message<Data, Meta>>): List<Message<Data, Meta>> {
        if (!Kolbasa.openTelemetryConfig.enabled) {
            return businessCall()
        }

        val start = Instant.now()
        val result = businessCall()
        val end = Instant.now()

        val parentContext = Context.current()
        if (consumerInstrumenter.shouldStart(parentContext, result)) {
            InstrumenterUtil.startAndEnd(consumerInstrumenter, parentContext, result, null, null, start, end)
        }

        return result
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

    private fun buildConsumerInstrumenter(
        openTelemetry: OpenTelemetry,
        queueName: String
    ): Instrumenter<List<Message<Data, Meta>>, Unit> {
        val getter = ConsumerResponseAttributesGetter<Data, Meta>(queueName)
        val operation = MessageOperation.RECEIVE

        val attributesExtractor = MessagingAttributesExtractor
            .builder(getter, operation)
            .build()

        return Instrumenter
            .builder<List<Message<Data, Meta>>, Unit>(
                openTelemetry,
                "kolbasa",
                MessagingSpanNameExtractor.create(getter, operation)
            )
            .addSpanLinksExtractor(
                ConsumerSpanLinksExtractor(
                    openTelemetry.propagators.textMapPropagator,
                    ContextFromMessageGetter()
                )
            )
            .addAttributesExtractor(attributesExtractor)
            .addAttributesExtractor(ConsumerResponseAttributesExtractor())
            .buildInstrumenter(SpanKindExtractor.alwaysConsumer())
    }


}
