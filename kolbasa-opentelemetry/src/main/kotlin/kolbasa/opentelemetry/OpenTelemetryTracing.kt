package kolbasa.opentelemetry

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
import kolbasa.producer.SendResult
import kolbasa.stats.tracing.ReceiveCall
import kolbasa.stats.tracing.SendCall
import kolbasa.stats.tracing.Tracing
import java.time.Instant
import java.util.function.Supplier

/**
 * [Tracing] with OpenTelemetry.
 *
 * Every `send()` becomes a producer span, and its trace context is stored with the sent messages. Every `receive()`
 * becomes a consumer span, linked to the spans that sent the received messages. So both sides of the queue can be
 * seen as one flow, even though they are different applications and different moments in time.
 *
 * One object serves all queues, so create it once and share it.
 *
 * ## Usage Example
 *
 * ```kotlin
 * // for the whole application
 * Kolbasa.tracing = OpenTelemetryTracing(openTelemetry)
 * // or for one producer only
 * val producer = DatabaseProducer(dataSource, ProducerOptions(tracing = OpenTelemetryTracing(openTelemetry)))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setTracing(new OpenTelemetryTracing(openTelemetry));
 * ```
 */
class OpenTelemetryTracing(
    /**
     * Where the spans go, and how the trace context is written into messages and read back.
     */
    openTelemetry: OpenTelemetry
) : Tracing {

    private val producerInstrumenter: Instrumenter<SendSpanInput, SendResult<*>> =
        buildProducerInstrumenter(openTelemetry)

    private val consumerInstrumenter: Instrumenter<ReceiveSpanInput, Unit> =
        buildConsumerInstrumenter(openTelemetry)

    override fun <Data> send(call: SendCall<Data>): SendResult<Data> {
        val spanInput = SendSpanInput(call.queue.name, call.nodeId, call.request)
        val parentContext = Context.current()
        if (!producerInstrumenter.shouldStart(parentContext, spanInput)) {
            return call.proceed(emptyMap())
        }

        // Start instrumenting. It also writes the new span context into spanInput
        val context = producerInstrumenter.start(parentContext, spanInput)
        val response = try {
            context.makeCurrent().use {
                call.proceed(spanInput.traceContext)
            }
        } catch (e: Exception) {
            producerInstrumenter.end(context, spanInput, null, e)
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
        producerInstrumenter.end(context, spanInput, response, errors.firstOrNull())

        return response
    }

    override fun <Data> receive(call: ReceiveCall<Data>): List<Message<Data>> {
        val start = Instant.now()
        val result = try {
            call.proceed()
        } catch (e: Exception) {
            // No messages, so the span has no links, only the error
            makeConsumerSpan(call, emptyList(), e, start)
            throw e
        }
        makeConsumerSpan(call, result, null, start)

        return result
    }

    override fun <T> wrapAsync(task: Supplier<T>): Supplier<T> {
        // The executor runs the task in another thread, where Context.current() is empty. Take the caller's context
        // along, so the send span gets the caller's span as its parent
        return Context.current().wrapSupplier(task)
    }

    // The span is made after the call, not before: its links come from the received messages
    private fun <Data> makeConsumerSpan(
        call: ReceiveCall<Data>,
        messages: List<Message<Data>>,
        error: Exception?,
        start: Instant
    ) {
        val end = Instant.now()

        val spanInput = ReceiveSpanInput(call.queue.name, call.nodeId, messages, messages.map(call::traceContext))
        val parentContext = Context.current()
        if (consumerInstrumenter.shouldStart(parentContext, spanInput)) {
            InstrumenterUtil.startAndEnd(consumerInstrumenter, parentContext, spanInput, null, error, start, end)
        }
    }

    private fun buildProducerInstrumenter(openTelemetry: OpenTelemetry): Instrumenter<SendSpanInput, SendResult<*>> {
        val getter = SendSpanAttributesGetter()
        val operationType = MessagingOperationType.SEND
        // Name of our own method, goes to "messaging.operation.name"
        val operationName = "send"

        val spanNameExtractor = MessagingSpanNameExtractor.create(getter, operationType, operationName)

        val messagingAttributesExtractor =
            MessagingAttributesExtractor.create(getter, operationType, operationName)

        val nodeIdAttributesExtractor = object : AttributesExtractor<SendSpanInput, SendResult<*>> {
            override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: SendSpanInput) {
                if (request.nodeId.id.isNotEmpty()) {
                    attributes.put(NODE_ID_ATTRIBUTE_KEY, request.nodeId.id)
                }
            }

            override fun onEnd(
                attributes: AttributesBuilder,
                context: Context,
                request: SendSpanInput,
                response: SendResult<*>?,
                error: Throwable?
            ) {
            }
        }

        return Instrumenter.builder<SendSpanInput, SendResult<*>>(
            openTelemetry,
            "kolbasa",
            spanNameExtractor
        )
            .addAttributesExtractor(messagingAttributesExtractor)
            .addAttributesExtractor(nodeIdAttributesExtractor)
            .buildProducerInstrumenter(TraceContextSetter())
    }

    private fun buildConsumerInstrumenter(openTelemetry: OpenTelemetry): Instrumenter<ReceiveSpanInput, Unit> {
        val getter = ReceiveSpanAttributesGetter()
        val operationType = MessagingOperationType.RECEIVE
        // Name of our own method, goes to "messaging.operation.name"
        val operationName = "receive"

        val messagingAttributesExtractor = MessagingAttributesExtractor.create(getter, operationType, operationName)

        val nodeIdAttributesExtractor = object : AttributesExtractor<ReceiveSpanInput, Unit> {
            override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: ReceiveSpanInput) {
                if (request.nodeId.id.isNotEmpty()) {
                    attributes.put(NODE_ID_ATTRIBUTE_KEY, request.nodeId.id)
                }
            }

            override fun onEnd(
                attributes: AttributesBuilder,
                context: Context,
                request: ReceiveSpanInput,
                response: Unit?,
                error: Throwable?
            ) {
            }
        }

        return Instrumenter.builder<ReceiveSpanInput, Unit>(
            openTelemetry,
            "kolbasa",
            MessagingSpanNameExtractor.create(getter, operationType, operationName)
        )
            .addSpanLinksExtractor(
                ReceiveSpanLinksExtractor(
                    openTelemetry.propagators.textMapPropagator,
                    TraceContextGetter()
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
