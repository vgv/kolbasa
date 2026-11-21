package kolbasa.opentelemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.*
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapPropagator
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
            makeConsumerSpan(call = call, messages = emptyList(), error = e, start = start)
            throw e
        }
        makeConsumerSpan(call = call, messages = result, error = null, start = start)

        return result
    }

    override fun <Data> process(call: ProcessCall<Data>): HandleResult {
        // The process span has a parent, and it may have a link. Two spans decide them:
        //   publish - the span that sent the message. Its trace context is stored with the message
        //   job     - the span that was current when QueueProcessor.process() was called, e.g. a scheduled job
        //
        // A box inside a box is a child span. It is not inside in time: a message may be processed hours/days after it
        // was sent.
        //
        // 1. Both spans are there. The parent is publish, and the link goes to job. So, looking at the processing
        //    of a message, you can find the job that did it
        //
        //    ┌ publish ────────────────┐
        //    │ ┌ process ────────────┐ │   link   ┌ job ────────────────────┐
        //    │ │                     ├─┼─────────>│                         │
        //    │ └─────────────────────┘ │          └─────────────────────────┘
        //    └─────────────────────────┘
        //
        // 2. Only publish, e.g. in a QueueWorker thread. The parent is publish, there is no link
        //
        //    ┌ publish ────────────────┐
        //    │ ┌ process ────────────┐ │
        //    │ └─────────────────────┘ │
        //    └─────────────────────────┘
        //
        // 3. Only job: the message was sent without tracing, or by the q_<queue>_put() SQL function. The parent is
        //    job, so it needs no link
        //
        //    ┌ job ────────────────────┐
        //    │ ┌ process ────────────┐ │
        //    │ └─────────────────────┘ │
        //    └─────────────────────────┘
        //
        // 4. Neither. The process span starts a new trace
        //
        //    ┌ process ────────────┐
        //    └─────────────────────┘
        val callerContext = Context.current()

        // extract() reads the publish span from the message and puts it into the caller's context, in place of the
        // job span (cases 1 and 2). A message without a trace context changes nothing: the job span stays the parent
        // (case 3), or there is no parent at all (case 4)
        val parentContext = propagator.extract(callerContext, call.traceContext(), TraceContextGetter())

        val queueName = call.queue.name
        val spanBuilder = tracer.spanBuilder("$queueName process")
            .setParent(parentContext)
            .setSpanKind(SpanKind.CONSUMER)
            .setMessagingAttributes(queueName, "process")
            .setAttribute(MESSAGING_MESSAGE_ID, call.message.id.toString())
            .setAttribute(KOLBASA_MESSAGE_LAST_ATTEMPT, call.message.isLastAttempt())

        // Case 1: the job span is there, but it is not the parent
        val callerSpanContext = Span.fromContext(callerContext).spanContext
        if (callerSpanContext.isValid && callerSpanContext != Span.fromContext(parentContext).spanContext) {
            spanBuilder.addLink(callerSpanContext)
        }

        // While the handler runs, the process span is the current one, so the handler's own spans become its children
        val span = spanBuilder.startSpan()
        val result = try {
            parentContext.with(span).makeCurrent().use {
                call.proceed()
            }
        } catch (e: Exception) {
            span.setAttribute(KOLBASA_PROCESS_RESULT, "failed")
            span.recordErrors(listOf(e))
            span.end()
            throw e
        }

        // The same words as the counters of ProcessResult, which QueueProcessor.process() returns
        val processResult = when (result) {
            HandleResult.DONE -> "succeeded"
            HandleResult.SKIP -> "skipped"
        }
        span.setAttribute(KOLBASA_PROCESS_RESULT, processResult)
        span.end()

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

        // An empty poll with no span around it says nothing, see the class KDoc. A failed receive is not empty, it
        // always makes its span
        val isEmptyPoll = messages.isEmpty() && error == null
        val hasParentSpan = Span.current().spanContext.isValid
        if (isEmptyPoll && !hasParentSpan) {
            return
        }

        val queueName = call.queue.name
        val spanBuilder = tracer.spanBuilder("$queueName receive")
            .setSpanKind(SpanKind.CONSUMER)
            .setStartTimestamp(start)
            .setMessagingAttributes(queueName, "receive")
            .setBatchMessageCount(messages.size)
            .setNodeId(call.nodeId)

        // Only for one message, as for send and process: a batch has no single id
        if (messages.size == 1) {
            spanBuilder.setAttribute(MESSAGING_MESSAGE_ID, messages[0].id.toString())
        }

        // A link to the span that sent each message. A message sent without tracing has no context, so no link
        messages.forEach { message ->
            val senderContext = propagator.extract(Context.root(), call.traceContext(message), TraceContextGetter())
            val senderSpanContext = Span.fromContext(senderContext).spanContext
            if (senderSpanContext.isValid) {
                spanBuilder.addLink(senderSpanContext)
            }
        }

        val span = spanBuilder.startSpan()
        if (error != null) {
            span.recordErrors(listOf(error))
        }
        span.end(end)
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
            setAttribute(KOLBASA_NODE_ID, nodeId.id)
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

    companion object {
        // The name of the tracer, it shows up in every span
        private const val INSTRUMENTATION_NAME = "kolbasa"

        // The semantic conventions 1.24.0, with the old names for messaging. On purpose: the OpenTelemetry Java
        // instrumentations (Kafka, RabbitMQ...) still emit these names by default, so in one application our spans
        // look like theirs. They move to the new names in their version 3.0, and we should move with them
        private const val SCHEMA_URL = "https://opentelemetry.io/schemas/1.24.0"

        // Attributes from the semantic conventions for messaging
        private val MESSAGING_SYSTEM = AttributeKey.stringKey("messaging.system")
        private val MESSAGING_DESTINATION_NAME = AttributeKey.stringKey("messaging.destination.name")
        private val MESSAGING_OPERATION = AttributeKey.stringKey("messaging.operation")
        private val MESSAGING_MESSAGE_ID = AttributeKey.stringKey("messaging.message.id")
        private val MESSAGING_BATCH_MESSAGE_COUNT = AttributeKey.longKey("messaging.batch.message_count")

        // Our own attributes
        private val KOLBASA_NODE_ID = AttributeKey.stringKey("kolbasa.node.id")
        private val KOLBASA_PROCESS_RESULT = AttributeKey.stringKey("kolbasa.process.result")
        private val KOLBASA_MESSAGE_LAST_ATTEMPT = AttributeKey.booleanKey("kolbasa.message.last_attempt")
    }
}
