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
import kolbasa.consumer.worker.HandleResult
import kolbasa.producer.SendResult
import kolbasa.telemetry.tracing.ProcessCall
import kolbasa.telemetry.tracing.ReceiveCall
import kolbasa.telemetry.tracing.SendCall
import kolbasa.telemetry.tracing.Tracing
import java.time.Instant
import java.util.function.Supplier

/**
 * [Tracing] with OpenTelemetry.
 *
 * Every `send()` becomes a producer span, and its trace context is stored with the sent messages. Every `receive()`
 * becomes a consumer span, linked to the spans that sent the received messages. So both sides of the queue can be
 * seen as one flow, even though they are different applications and different moments in time.
 *
 * With a [QueueProcessor][kolbasa.consumer.worker.QueueProcessor], the handling of every message becomes a span too.
 * Its parent is the span that sent the message, so all the handler does, its SQL and HTTP calls for example, lands
 * in the sender's trace. If the processor runs inside a span of yours, for example a scheduled job, the process span
 * links to it. The process span tells how the handling ended in the attribute `kolbasa.process.result`:
 * `succeeded`, `skipped` or `failed`. The attribute `kolbasa.message.last_attempt` is `true` on the message's last
 * attempt: if that one fails or is skipped, the message goes to the DLQ, or is deleted if the queue has none.
 *
 * A `receive()` that finds no messages makes no span when there is no span around it, as in a
 * [QueueWorker][kolbasa.consumer.worker.QueueWorker] thread that polls an empty queue. Such a span would say nothing
 * and only flood the traces, a new one-span trace on every poll. Inside a span of yours, an empty `receive()` still
 * makes its span, so a scheduled job shows that it found nothing. To see that a worker is alive, look at the metric
 * `kolbasa_consumer_receive` instead: it counts every `receive()`, the empty ones too.
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

    private val processInstrumenter: Instrumenter<ProcessSpanInput, HandleResult> =
        buildProcessInstrumenter(openTelemetry)

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

    override fun <Data> process(call: ProcessCall<Data>): HandleResult {
        val parentContext = Context.current()
        val callerSpanContext = Span.fromContext(parentContext).spanContext
        val spanInput = ProcessSpanInput(call.queue.name, call.message, call.traceContext(), callerSpanContext)
        if (!processInstrumenter.shouldStart(parentContext, spanInput)) {
            return call.proceed()
        }

        // The instrumenter takes the sender's context from the message and makes it the parent. While the handler
        // runs, the process span is the current one, so the handler's own spans become its children
        val context = processInstrumenter.start(parentContext, spanInput)
        val result = try {
            context.makeCurrent().use {
                call.proceed()
            }
        } catch (e: Exception) {
            processInstrumenter.end(context, spanInput, null, e)
            throw e
        }
        processInstrumenter.end(context, spanInput, result, null)

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
        val parentContext = Context.current()

        // An empty poll with no span around it says nothing, see the class KDoc. A failed receive is not empty, it
        // always makes its span
        val isEmptyPoll = messages.isEmpty() && error == null
        val hasParentSpan = Span.fromContext(parentContext).spanContext.isValid
        if (isEmptyPoll && !hasParentSpan) {
            return
        }

        val spanInput = ReceiveSpanInput(call.queue.name, call.nodeId, messages, messages.map(call::traceContext))
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

    private fun buildProcessInstrumenter(openTelemetry: OpenTelemetry): Instrumenter<ProcessSpanInput, HandleResult> {
        val getter = ProcessSpanAttributesGetter()
        val operationType = MessagingOperationType.PROCESS
        // Name of the operation, goes to "messaging.operation.name"
        val operationName = "process"

        // Whether this is the message's last attempt, and how the handling ended
        val processAttributesExtractor = object : AttributesExtractor<ProcessSpanInput, HandleResult> {
            override fun onStart(attributes: AttributesBuilder, parentContext: Context, request: ProcessSpanInput) {
                attributes.put(LAST_ATTEMPT_ATTRIBUTE_KEY, request.message.isLastAttempt())
            }

            override fun onEnd(
                attributes: AttributesBuilder,
                context: Context,
                request: ProcessSpanInput,
                response: HandleResult?,
                error: Throwable?
            ) {
                // The same words as in ProcessResult
                val result = when {
                    error != null -> "failed"
                    response == HandleResult.DONE -> "succeeded"
                    response == HandleResult.SKIP -> "skipped"
                    else -> return
                }
                attributes.put(PROCESS_RESULT_ATTRIBUTE_KEY, result)
            }
        }

        return Instrumenter.builder<ProcessSpanInput, HandleResult>(
            openTelemetry,
            "kolbasa",
            MessagingSpanNameExtractor.create(getter, operationType, operationName)
        )
            .addSpanLinksExtractor(ProcessSpanLinksExtractor())
            .addAttributesExtractor(MessagingAttributesExtractor.create(getter, operationType, operationName))
            .addAttributesExtractor(processAttributesExtractor)
            // A consumer span whose parent comes from the message
            .buildConsumerInstrumenter(ProcessTraceContextGetter())
    }

    companion object {
        private val NODE_ID_ATTRIBUTE_KEY = AttributeKey.stringKey("kolbasa.node.id")
        private val PROCESS_RESULT_ATTRIBUTE_KEY = AttributeKey.stringKey("kolbasa.process.result")
        private val LAST_ATTEMPT_ATTRIBUTE_KEY= AttributeKey.booleanKey("kolbasa.message.last_attempt")
    }
}
