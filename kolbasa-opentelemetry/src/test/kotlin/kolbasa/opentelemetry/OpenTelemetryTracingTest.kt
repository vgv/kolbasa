package kolbasa.opentelemetry

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.LinkData
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.MessageOptions
import kolbasa.producer.PartialInsert
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.test.assertNotNull
import kolbasa.utils.JdbcHelpers.inTransaction
import kolbasa.worker.HandleResult
import kolbasa.worker.MessageHandler
import kolbasa.worker.QueueProcessor
import kolbasa.worker.QueueProcessorOptions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException

class OpenTelemetryTracingTest : AbstractPostgreSQLTest() {

    private val spanExporter = InMemorySpanExporter.create()
    private val tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
        .build()

    private val tracing = OpenTelemetryTracing(
        OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build()
    )

    // Tracing is set per producer and consumer, not globally, so other tests running meanwhile are not traced
    private val producerOptions = ProducerOptions(tracing = tracing)
    private val consumerOptions = ConsumerOptions(tracing = tracing)

    private val queue = Queue.of("tracing", PredefinedDataTypes.String)

    private val PROCESS_RESULT = AttributeKey.stringKey("kolbasa.process.result")
    private val MESSAGE_ID = AttributeKey.stringKey("messaging.message.id")
    private val LAST_ATTEMPT = AttributeKey.booleanKey("kolbasa.message.last_attempt")

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @AfterEach
    fun after() {
        tracerProvider.close()
    }

    @Test
    fun testSendAndReceive_Database() {
        DatabaseProducer(dataSource, producerOptions).send(queue, "bugaga").throwExceptionIfAny()
        assertNotNull(DatabaseConsumer(dataSource, consumerOptions).receive(queue))

        assertOneLinkedSpanPerCall()
    }

    @Test
    fun testSendAndReceive_ConnectionAware() {
        val producer = ConnectionAwareDatabaseProducer(producerOptions)
        val consumer = ConnectionAwareDatabaseConsumer(consumerOptions)
        dataSource.inTransaction { producer.send(it, queue, "bugaga").throwExceptionIfAny() }
        dataSource.inTransaction { assertNotNull(consumer.receive(it, queue)) }

        assertOneLinkedSpanPerCall()
    }

    @Test
    fun testSend_MessageId() {
        // One message: the publish span holds its id, the same as the process span of this message
        val id = DatabaseProducer(dataSource, producerOptions).send(queue, "bugaga").onlySuccessful().single().id
        processor { HandleResult.DONE }.process()

        val publish = spanExporter.finishedSpanItems.single { it.name == "tracing publish" }
        val process = spanExporter.finishedSpanItems.single { it.name == "tracing process" }
        assertEquals(id.toString(), publish.attributes.get(MESSAGE_ID))
        assertEquals(id.toString(), process.attributes.get(MESSAGE_ID))
    }

    @Test
    fun testSend_MessageId_Batch() {
        // A batch has no single id, so the publish span has none
        val request = SendRequest(listOf(SendMessage("bugaga1"), SendMessage("bugaga2")))
        DatabaseProducer(dataSource, producerOptions).send(queue, request).throwExceptionIfAny()

        val publish = spanExporter.finishedSpanItems.single { it.name == "tracing publish" }
        assertNull(publish.attributes.get(MESSAGE_ID))
    }

    @Test
    fun testSend_SameRequestTwice() {
        // Sending the same request again is legal, e.g. a retry. Each message must carry only its own trace context
        val request = SendRequest(listOf(SendMessage("bugaga")))
        val producer = DatabaseProducer(dataSource, producerOptions)
        producer.send(queue, request).throwExceptionIfAny()
        producer.send(queue, request).throwExceptionIfAny()

        val expected = spanExporter.finishedSpanItems.map { span ->
            "traceparent 00-${span.traceId}-${span.spanId}-${span.spanContext.traceFlags.asHex()}"
        }

        val contexts = readStrings("select array_to_string(opentelemetry, ' ') from q_tracing order by id")
        assertEquals(expected, contexts)
    }

    @Test
    fun testSend_Error() {
        // No queue table, so the insert fails
        SchemaHelpers.deleteQueues(dataSource, queue)

        // send() doesn't throw, it returns the error inside SendResult
        val result = DatabaseProducer(dataSource, producerOptions).send(queue, "bugaga")
        assertEquals(1, result.failedMessages)

        // The span is marked as an error anyway, and holds the exception
        val spans = spanExporter.finishedSpanItems
        assertEquals(1, spans.size, spans.joinToString { "${it.kind} '${it.name}'" })
        val producerSpan = spans.first()
        assertEquals(SpanKind.PRODUCER, producerSpan.kind)
        assertEquals("tracing publish", producerSpan.name)
        assertEquals(StatusCode.ERROR, producerSpan.status.statusCode)
        assertEquals(listOf("exception"), producerSpan.events.map { it.name })
    }

    @Test
    fun testSend_SeveralErrors() {
        // No queue table, so every chunk fails, each with its own exception
        SchemaHelpers.deleteQueues(dataSource, queue)

        val options = SendOptions(chunkSize = 1, partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)
        val request = SendRequest(listOf(SendMessage("bugaga1"), SendMessage("bugaga2")), options = options)
        val result = DatabaseProducer(dataSource, producerOptions).send(queue, request)
        assertEquals(2, result.failedMessages)

        // The span holds all of them
        val producerSpan = spanExporter.finishedSpanItems.single()
        assertEquals(StatusCode.ERROR, producerSpan.status.statusCode)
        assertEquals(listOf("exception", "exception"), producerSpan.events.map { it.name })
    }

    @Test
    fun testSendAsync_ParentSpan() {
        // sendAsync() sends in another thread. The send span must still get the caller's span as its parent
        val parentSpan = tracerProvider.get("test").spanBuilder("parent").startSpan()
        parentSpan.makeCurrent().use {
            DatabaseProducer(dataSource, producerOptions)
                .sendAsync(queue, "bugaga")
                .get()
                .throwExceptionIfAny()
        }
        parentSpan.end()

        // Only the caller's span and the send span, nothing else
        val spans = spanExporter.finishedSpanItems
        assertEquals(2, spans.size, spans.joinToString { "${it.kind} '${it.name}'" })
        val producerSpan = spans.single { it.kind == SpanKind.PRODUCER }
        assertEquals(parentSpan.spanContext.spanId, producerSpan.parentSpanId)
    }

    @Test
    fun testReceive_Error() {
        // No queue table, so the receive query fails
        SchemaHelpers.deleteQueues(dataSource, queue)

        assertThrows<SQLException> { DatabaseConsumer(dataSource, consumerOptions).receive(queue) }

        // The failed receive() still makes its span, and the span is marked as an error
        val spans = spanExporter.finishedSpanItems
        assertEquals(1, spans.size, spans.joinToString { "${it.kind} '${it.name}'" })
        val consumerSpan = spans.first()
        assertEquals(SpanKind.CONSUMER, consumerSpan.kind)
        assertEquals("tracing receive", consumerSpan.name)
        assertEquals(StatusCode.ERROR, consumerSpan.status.statusCode)
    }

    @Test
    fun testReceive_Empty_NoSpan() {
        // The queue is empty and there is no span around, as in a worker thread: such a poll makes no span
        assertNull(DatabaseConsumer(dataSource, consumerOptions).receive(queue))
        assertEquals(emptyList<SpanData>(), spanExporter.finishedSpanItems)
    }

    @Test
    fun testReceive_Empty_InsideASpan() {
        // Inside a span of the caller, an empty poll still makes its span: the job found nothing
        val job = tracerProvider.get("test").spanBuilder("job").startSpan()
        job.makeCurrent().use { assertNull(DatabaseConsumer(dataSource, consumerOptions).receive(queue)) }
        job.end()

        val receive = spanExporter.finishedSpanItems.single { it.name == "tracing receive" }
        assertEquals(job.spanContext.spanId, receive.parentSpanId)
    }

    @Test
    fun testProcess_ParentIsTheSender() {
        DatabaseProducer(dataSource, producerOptions).send(queue, "bugaga").throwExceptionIfAny()

        // The handler makes a span of its own, as its SQL or HTTP calls would
        val processor = processor {
            tracerProvider.get("test").spanBuilder("handler").startSpan().end()
            HandleResult.DONE
        }
        assertEquals(1, processor.process().succeeded)

        val spans = spanExporter.finishedSpanItems
        val publish = spans.single { it.name == "tracing publish" }
        val process = spans.single { it.name == "tracing process" }
        val handler = spans.single { it.name == "handler" }

        // The process span is in the sender's trace, and its parent is the publish span
        assertEquals(SpanKind.CONSUMER, process.kind)
        assertEquals(publish.traceId, process.traceId)
        assertEquals(publish.spanId, process.parentSpanId)

        // The handler's span is a child of the process span
        assertEquals(process.spanId, handler.parentSpanId)

        // process() wasn't called inside a span, so there is nothing to link to
        assertEquals(emptyList<LinkData>(), process.links)

        // And the span tells how the handling ended. The message has more attempts, so it wasn't its last one
        assertEquals("succeeded", process.attributes.get(PROCESS_RESULT))
        assertEquals(false, process.attributes.get(LAST_ATTEMPT))
    }

    @Test
    fun testProcess_LinksToTheCaller() {
        DatabaseProducer(dataSource, producerOptions).send(queue, "bugaga").throwExceptionIfAny()

        // process() runs inside a span of the caller, as a scheduled job would
        val job = tracerProvider.get("test").spanBuilder("job").startSpan()
        job.makeCurrent().use { processor { HandleResult.DONE }.process() }
        job.end()

        val spans = spanExporter.finishedSpanItems
        val publish = spans.single { it.name == "tracing publish" }
        val receive = spans.single { it.name == "tracing receive" }
        val process = spans.single { it.name == "tracing process" }

        // The receive span is in the job's trace
        assertEquals(job.spanContext.spanId, receive.parentSpanId)

        // The process span stays in the sender's trace, and links to the job
        assertEquals(publish.spanId, process.parentSpanId)
        assertEquals(listOf(job.spanContext.spanId), process.links.map { it.spanContext.spanId })
    }

    @Test
    fun testProcess_NoSenderContext() {
        // Sent without tracing, so the message has no trace context
        DatabaseProducer(dataSource).send(queue, "bugaga").throwExceptionIfAny()

        val job = tracerProvider.get("test").spanBuilder("job").startSpan()
        job.makeCurrent().use { processor { HandleResult.DONE }.process() }
        job.end()

        // Then the process span stays in the caller's trace, and the parent needs no link
        val process = spanExporter.finishedSpanItems.single { it.name == "tracing process" }
        assertEquals(job.spanContext.spanId, process.parentSpanId)
        assertEquals(emptyList<LinkData>(), process.links)
    }

    @Test
    fun testProcess_Error() {
        DatabaseProducer(dataSource, producerOptions).send(queue, "bugaga").throwExceptionIfAny()

        val processor = processor { throw IllegalArgumentException("Can't handle") }
        assertEquals(1, processor.process().failed)

        // The failed handler marks the process span as an error, and the span holds the exception
        val process = spanExporter.finishedSpanItems.single { it.name == "tracing process" }
        assertEquals(StatusCode.ERROR, process.status.statusCode)
        assertEquals(listOf("exception"), process.events.map { it.name })
        assertEquals("failed", process.attributes.get(PROCESS_RESULT))
    }

    @Test
    fun testProcess_LastAttempt() {
        // Only one attempt, so the first handling is the last one
        val message = SendMessage("bugaga", MessageOptions(attempts = 1))
        DatabaseProducer(dataSource, producerOptions).send(queue, message).throwExceptionIfAny()

        val processor = processor { throw IllegalArgumentException("Can't handle") }
        assertEquals(1, processor.process().failed)

        // The message fails on its last attempt, so it is about to leave the queue
        val process = spanExporter.finishedSpanItems.single { it.name == "tracing process" }
        assertEquals("failed", process.attributes.get(PROCESS_RESULT))
        assertEquals(true, process.attributes.get(LAST_ATTEMPT))
    }

    @Test
    fun testProcess_Skip() {
        DatabaseProducer(dataSource, producerOptions).send(queue, "bugaga").throwExceptionIfAny()

        val processor = processor { HandleResult.SKIP }
        assertEquals(1, processor.process().skipped)

        // A skipped message is not an error, but the span says it was skipped
        val process = spanExporter.finishedSpanItems.single { it.name == "tracing process" }
        assertEquals(StatusCode.UNSET, process.status.statusCode)
        assertEquals("skipped", process.attributes.get(PROCESS_RESULT))
    }

    // One send() makes one producer span, one receive() makes one consumer span, and the consumer span links to
    // the producer one: the sender's trace context travels inside the message
    private fun assertOneLinkedSpanPerCall() {
        val spans = spanExporter.finishedSpanItems
        val allSpans = spans.joinToString { "${it.kind} '${it.name}'" }

        val producerSpans = spans.filter { it.kind == SpanKind.PRODUCER }
        assertEquals(1, producerSpans.size, allSpans)
        val producerSpan = producerSpans.first()
        assertEquals("tracing publish", producerSpan.name)

        val consumerSpans = spans.filter { it.kind == SpanKind.CONSUMER }
        assertEquals(1, consumerSpans.size, allSpans)
        val consumerSpan = consumerSpans.first()
        assertEquals("tracing receive", consumerSpan.name)

        // Compare the ids only: the link holds a remote copy of the producer span context
        val links = consumerSpan.links.map { it.spanContext.traceId to it.spanContext.spanId }
        assertEquals(listOf(producerSpan.traceId to producerSpan.spanId), links)
    }

    // A processor over the traced consumer, so it is traced too. Its errors go nowhere, the tests check the spans
    private fun processor(handler: MessageHandler<String>): QueueProcessor<String> {
        val options = QueueProcessorOptions(errorHandler = { })
        return QueueProcessor(queue, DatabaseConsumer(dataSource, consumerOptions), options, handler)
    }

    private fun readStrings(query: String): List<String> = dataSource.connection.use { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(query).use { resultSet ->
                buildList {
                    while (resultSet.next())
                        add(resultSet.getString(1))
                }
            }
        }
    }

}
