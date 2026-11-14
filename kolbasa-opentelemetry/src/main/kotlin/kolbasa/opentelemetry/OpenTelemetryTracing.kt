package kolbasa.opentelemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributesBuilder
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingOperationType
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor
import io.opentelemetry.instrumentation.api.internal.InstrumenterUtil
import kolbasa.consumer.Message
import kolbasa.producer.MessageResult
import kolbasa.producer.SendResult
import kolbasa.schema.NodeId
import kolbasa.telemetry.tracing.ProcessCall
import kolbasa.telemetry.tracing.ReceiveCall
import kolbasa.telemetry.tracing.SendCall
import kolbasa.telemetry.tracing.Tracing
import kolbasa.worker.processor.HandleResult
import java.time.Instant
import java.util.function.Supplier

/**
 * [Tracing] with OpenTelemetry.
 *
 * Every `send()` becomes a producer span, and its trace context is stored with the sent messages. Every `receive()`
 * becomes a consumer span, linked to the spans that sent the received messages. So both sides of the queue can be
 * seen as one flow, even though they are different applications and different moments in time.
 *
 * With a [QueueProcessor][kolbasa.worker.processor.QueueProcessor], the handling of every message becomes a span too.
 * Its parent is the span that sent the message, so all the handler does, its SQL and HTTP calls for example, lands
 * in the sender's trace. If the processor runs inside a span of yours, for example a scheduled job, the process span
 * links to it. The process span tells how the handling ended in the attribute `kolbasa.process.result`:
 * `succeeded`, `skipped` or `failed`. The attribute `kolbasa.message.last_attempt` is `true` on the message's last
 * attempt: if that one fails or is skipped, the message goes to the DLQ, or is deleted if the queue has none.
 *
 * A `receive()` that finds no messages makes no span when there is no span around it, as in a
 * [QueueWorker][kolbasa.worker.QueueWorker] thread that polls an empty queue. Such a span would say nothing
 * and only flood the traces, a new one-span trace on every poll. Inside a span of yours, an empty `receive()` still
 * makes its span, so a scheduled job shows that it found nothing. To see that a worker is alive, look at the metric
 * `kolbasa.consumer.receive` instead (`kolbasa_consumer_receive_total` in Prometheus): it counts every `receive()`,
 * the empty ones too.
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

    private val tracer: Tracer = openTelemetry.tracerProvider
        .tracerBuilder(INSTRUMENTATION_NAME)
        // Which version of the semantic conventions the names and attributes of the spans follow
        .setSchemaUrl(SCHEMA_URL)
        .build()

    private val propagator: TextMapPropagator = openTelemetry.propagators.textMapPropagator

    private val consumerInstrumenter: Instrumenter<ReceiveSpanInput, Unit> =
        buildConsumerInstrumenter(openTelemetry)

    private val processInstrumenter: Instrumenter<ProcessSpanInput, HandleResult> =
        buildProcessInstrumenter(openTelemetry)

    override fun <Data> send(call: SendCall<Data>): SendResult<Data> {
        val queueName = call.queue.name
        val messageCount = call.request.data.size
        val span = tracer.spanBuilder("$queueName publish")
            .setSpanKind(SpanKind.PRODUCER)
            .setMessagingAttributes(queueName, "publish")
            .setBatchMessageCount(messageCount)
            .setNodeId(call.nodeId)
            .startSpan()

        // The context of the new span goes into the messages, so the receiving side can find this span
        val context = Context.current().with(span)
        val traceContext = mutableMapOf<String, String>()
        propagator.inject(context, traceContext, TraceContextSetter())

        val response = try {
            context.makeCurrent().use {
                call.proceed(traceContext)
            }
        } catch (e: Exception) {
            span.recordErrors(listOf(e))
            span.end()
            throw e
        }

        // Only for one message, as for receive and process: a batch has no single id.
        // And only if it was sent - a duplicate or a failed message has no id
        if (messageCount == 1) {
            val result = response.messages.singleOrNull() as? MessageResult.Success<*>
            if (result != null) {
                span.setAttribute(MESSAGING_MESSAGE_ID, result.id.toString())
            }
        }

        // A failed send doesn't throw, it returns its errors inside the response. They go to the span all the same
        span.recordErrors(response.gatherExceptions())
        span.end()

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

    // The attributes every span has: who made it, which queue, and what was done
    private fun SpanBuilder.setMessagingAttributes(queueName: String, operation: String): SpanBuilder {
        return setAttribute(MESSAGING_SYSTEM, "kolbasa")
            .setAttribute(MESSAGING_DESTINATION_NAME, queueName)
            .setAttribute(MESSAGING_OPERATION, operation)
    }

    // Only for a batch: one message has its id instead
    private fun SpanBuilder.setBatchMessageCount(messageCount: Int): SpanBuilder {
        return if (messageCount > 1) {
            setAttribute(MESSAGING_BATCH_MESSAGE_COUNT, messageCount.toLong())
        } else {
            this
        }
    }

    // Only in a cluster: outside of one, the node id is empty
    private fun SpanBuilder.setNodeId(nodeId: NodeId): SpanBuilder {
        return if (nodeId.id.isNotEmpty()) {
            setAttribute(NODE_ID_ATTRIBUTE_KEY, nodeId.id)
        } else {
            this
        }
    }

    // Each error becomes an "exception" event of the span, and one error is enough to mark the whole span as failed
    private fun Span.recordErrors(errors: List<Throwable>) {
        if (errors.isEmpty()) {
            return
        }

        errors.forEach { error -> recordException(error) }
        setStatus(StatusCode.ERROR)
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
        // The name of the tracer, it shows up in every span
        private const val INSTRUMENTATION_NAME = "kolbasa"
        private const val SCHEMA_URL = "https://opentelemetry.io/schemas/1.24.0"

        // Attributes from the semantic conventions for messaging
        private val MESSAGING_SYSTEM = AttributeKey.stringKey("messaging.system")
        private val MESSAGING_DESTINATION_NAME = AttributeKey.stringKey("messaging.destination.name")
        private val MESSAGING_OPERATION = AttributeKey.stringKey("messaging.operation")
        private val MESSAGING_MESSAGE_ID = AttributeKey.stringKey("messaging.message.id")
        private val MESSAGING_BATCH_MESSAGE_COUNT = AttributeKey.longKey("messaging.batch.message_count")

        // Our own attributes
        private val NODE_ID_ATTRIBUTE_KEY = AttributeKey.stringKey("kolbasa.node.id")
        private val PROCESS_RESULT_ATTRIBUTE_KEY = AttributeKey.stringKey("kolbasa.process.result")
        private val LAST_ATTEMPT_ATTRIBUTE_KEY= AttributeKey.booleanKey("kolbasa.message.last_attempt")
    }
}
