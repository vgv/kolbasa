package kolbasa.stats.opentelemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributesBuilder
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessageOperation
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor
import io.opentelemetry.instrumentation.api.internal.InstrumenterUtil
import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.schema.NodeId
import java.time.Instant

internal class OpenTelemetryQueueTracing<Data>(
    queueName: String,
    openTelemetryConfig: OpenTelemetryConfig.Config
): QueueTracing<Data> {

    private val producerInstrumenter: Instrumenter<ProducerCall<Data>, SendResult<Data>> =
        buildProducerInstrumenter(openTelemetryConfig.openTelemetry, queueName)

    private val consumerInstrumenter: Instrumenter<ConsumerCall<Data>, Unit> =
        buildConsumerInstrumenter(openTelemetryConfig.openTelemetry, queueName)

    override fun readOpenTelemetryData(): Boolean {
        return true
    }

    override fun makeProducerCall(
        nodeId: NodeId,
        request: SendRequest<Data>,
        businessCall: () -> SendResult<Data>
    ): SendResult<Data> {
        val producerCall = ProducerCall(request, nodeId)
        val parentContext = Context.current()
        if (!producerInstrumenter.shouldStart(parentContext, producerCall)) {
            return businessCall()
        }

        // Start instrumenting
        val context = producerInstrumenter.start(parentContext, producerCall)
        val response = try {
            context.makeCurrent().use {
                businessCall()
            }
        } catch (e: Exception) {
            producerInstrumenter.end(context, producerCall, null, e)
            throw e
        }
        producerInstrumenter.end(context, producerCall, response, null)

        return response
    }

    override fun makeConsumerCall(nodeId: NodeId, businessCall: () -> List<Message<Data>>): List<Message<Data>> {
        val start = Instant.now()
        val result = businessCall()
        val end = Instant.now()

        val consumerCall = ConsumerCall(result, nodeId)
        val parentContext = Context.current()
        if (consumerInstrumenter.shouldStart(parentContext, consumerCall)) {
            InstrumenterUtil.startAndEnd(consumerInstrumenter, parentContext, consumerCall, null, null, start, end)
        }

        return result
    }

    private fun buildProducerInstrumenter(
        openTelemetry: OpenTelemetry,
        queueName: String
    ): Instrumenter<ProducerCall<Data>, SendResult<Data>> {
        val sendRequestAttributesGetter = SendRequestAttributesGetter<Data>(queueName)

        val spanNameExtractor = MessagingSpanNameExtractor.create(sendRequestAttributesGetter, MessageOperation.PUBLISH)

        val messagingAttributesExtractor = MessagingAttributesExtractor.builder(
            sendRequestAttributesGetter,
            MessageOperation.PUBLISH
        )
            .build()

        val nodeIdAttributesExtractor = object : AttributesExtractor<ProducerCall<Data>, SendResult<Data>> {
            override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: ProducerCall<Data>) {
                if (request.nodeId.id.isNotEmpty()) {
                    attributes.put(NODE_ID_ATTRIBUTE_KEY, request.nodeId.id)
                }
            }

            override fun onEnd(
                attributes: AttributesBuilder,
                context: Context,
                request: ProducerCall<Data>,
                response: SendResult<Data>?,
                error: Throwable?
            ) {}
        }

        return Instrumenter.builder<ProducerCall<Data>, SendResult<Data>>(
            openTelemetry,
            "kolbasa",
            spanNameExtractor
        )
            .addAttributesExtractor(messagingAttributesExtractor)
            .addAttributesExtractor(nodeIdAttributesExtractor)
            .buildProducerInstrumenter(ContextToMessageSetter())
    }

    private fun buildConsumerInstrumenter(
        openTelemetry: OpenTelemetry,
        queueName: String
    ): Instrumenter<ConsumerCall<Data>, Unit> {
        val getter = ConsumerResponseAttributesGetter<Data>(queueName)
        val operation = MessageOperation.RECEIVE

        val messagingAttributesExtractor = MessagingAttributesExtractor.builder(getter, operation)
            .build()

        val nodeIdAttributesExtractor = object : AttributesExtractor<ConsumerCall<Data>, Unit> {
            override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: ConsumerCall<Data>) {
                if (request.nodeId.id.isNotEmpty()) {
                    attributes.put(NODE_ID_ATTRIBUTE_KEY, request.nodeId.id)
                }
            }

            override fun onEnd(
                attributes: AttributesBuilder,
                context: Context,
                request: ConsumerCall<Data>,
                response: Unit?,
                error: Throwable?
            ) {}
        }

        return Instrumenter.builder<ConsumerCall<Data>, Unit>(
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
            .addAttributesExtractor(messagingAttributesExtractor)
            .addAttributesExtractor(nodeIdAttributesExtractor)
            .buildInstrumenter(SpanKindExtractor.alwaysConsumer())
    }

    companion object {
        private val NODE_ID_ATTRIBUTE_KEY: AttributeKey<String> = AttributeKey.stringKey("kolbasa.node.id")
    }
}
