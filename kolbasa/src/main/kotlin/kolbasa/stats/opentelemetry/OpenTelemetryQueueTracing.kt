package kolbasa.stats.opentelemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributesBuilder
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingOperationType
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
) : QueueTracing<Data> {

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
        businessCall: (openTelemetryContext: List<String>?) -> SendResult<Data>
    ): SendResult<Data> {
        val producerCall = ProducerCall(request, nodeId)
        val parentContext = Context.current()
        if (!producerInstrumenter.shouldStart(parentContext, producerCall)) {
            return businessCall(null)
        }

        // Start instrumenting. It also writes the new span context into producerCall
        val context = producerInstrumenter.start(parentContext, producerCall)
        val openTelemetryContext = producerCall.openTelemetryContext.takeIf { it.isNotEmpty() }
        val response = try {
            context.makeCurrent().use {
                businessCall(openTelemetryContext)
            }
        } catch (e: Exception) {
            producerInstrumenter.end(context, producerCall, null, e)
            throw e
        }

        // A failed send doesn't throw, it returns its errors inside the response. Pass them on, so the span is marked
        // as an error too. The instrumenter.end() takes only one error, the others should go to the span directly, e.g.
        // 1) first exception goes to the instrumenter.end() call
        // 2) other exceptions (if any) go to the span directly by span.recordException()
        val errors = response.gatherExceptions()
        val span = Span.fromContext(context)
        for (i in 1 until errors.size) {
            span.recordException(errors[i])
        }
        producerInstrumenter.end(context, producerCall, response, errors.firstOrNull())

        return response
    }

    override fun makeConsumerCall(nodeId: NodeId, businessCall: () -> List<Message<Data>>): List<Message<Data>> {
        val start = Instant.now()
        val result = try {
            businessCall()
        } catch (e: Exception) {
            // No messages, so the span has no links, only the error
            makeConsumerSpan(nodeId, emptyList(), e, start)
            throw e
        }
        makeConsumerSpan(nodeId, result, null, start)

        return result
    }

    // The span is made after the call, not before: its links come from the received messages
    private fun makeConsumerSpan(nodeId: NodeId, messages: List<Message<Data>>, error: Exception?, start: Instant) {
        val end = Instant.now()

        val consumerCall = ConsumerCall(messages, nodeId)
        val parentContext = Context.current()
        if (consumerInstrumenter.shouldStart(parentContext, consumerCall)) {
            InstrumenterUtil.startAndEnd(consumerInstrumenter, parentContext, consumerCall, null, error, start, end)
        }
    }

    private fun buildProducerInstrumenter(
        openTelemetry: OpenTelemetry,
        queueName: String
    ): Instrumenter<ProducerCall<Data>, SendResult<Data>> {
        val sendRequestAttributesGetter = SendRequestAttributesGetter<Data>(queueName)
        val operationType = MessagingOperationType.SEND
        // Name of our own method, goes to "messaging.operation.name"
        val operationName = "send"

        val spanNameExtractor = MessagingSpanNameExtractor.create(sendRequestAttributesGetter, operationType, operationName)

        val messagingAttributesExtractor =
            MessagingAttributesExtractor.create(sendRequestAttributesGetter, operationType, operationName)

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
            ) {
            }
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
        val operationType = MessagingOperationType.RECEIVE
        // Name of our own method, goes to "messaging.operation.name"
        val operationName = "receive"

        val messagingAttributesExtractor = MessagingAttributesExtractor.create(getter, operationType, operationName)

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
            ) {
            }
        }

        return Instrumenter.builder<ConsumerCall<Data>, Unit>(
            openTelemetry,
            "kolbasa",
            MessagingSpanNameExtractor.create(getter, operationType, operationName)
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
