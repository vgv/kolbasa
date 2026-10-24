package kolbasa.stats.opentelemetry

import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kolbasa.Kolbasa
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.PartialInsert
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
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.parallel.Isolated
import java.sql.SQLException

// Kolbasa.openTelemetryConfig is global: while it is set, every queue used for the first time picks it up.
// So the test runs alone, and no other test can touch its queues meanwhile
@Isolated
class OpenTelemetryTracingTest : AbstractPostgreSQLTest() {

    private val spanExporter = InMemorySpanExporter.create()
    private val tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
        .build()

    private lateinit var queue: Queue<String>

    @BeforeEach
    fun before() {
        Kolbasa.openTelemetryConfig = OpenTelemetryConfig.Config(
            OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build()
        )

        // A queue reads the config once, on its first use, so it has to be created after the config is set
        queue = Queue.of("tracing", PredefinedDataTypes.String)
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @AfterEach
    fun after() {
        Kolbasa.openTelemetryConfig = OpenTelemetryConfig.None
        tracerProvider.close()
    }

    @Test
    fun testSendAndReceive_Database() {
        DatabaseProducer(dataSource).send(queue, "bugaga").throwExceptionIfAny()
        assertNotNull(DatabaseConsumer(dataSource).receive(queue))

        assertOneLinkedSpanPerCall()
    }

    @Test
    fun testSendAndReceive_ConnectionAware() {
        dataSource.inTransaction { ConnectionAwareDatabaseProducer().send(it, queue, "bugaga").throwExceptionIfAny() }
        dataSource.inTransaction { assertNotNull(ConnectionAwareDatabaseConsumer().receive(it, queue)) }

        assertOneLinkedSpanPerCall()
    }

    @Test
    fun testSend_SameRequestTwice() {
        // Sending the same request again is legal, e.g. a retry. Each message must carry only its own trace context
        val request = SendRequest(listOf(SendMessage("bugaga")))
        val producer = DatabaseProducer(dataSource)
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
        val result = DatabaseProducer(dataSource).send(queue, "bugaga")
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
        val result = DatabaseProducer(dataSource).send(queue, request)
        assertEquals(2, result.failedMessages)

        // The span holds all of them
        val producerSpan = spanExporter.finishedSpanItems.single()
        assertEquals(StatusCode.ERROR, producerSpan.status.statusCode)
        assertEquals(listOf("exception", "exception"), producerSpan.events.map { it.name })
    }

    @Test
    fun testReceive_Error() {
        // No queue table, so the receive query fails
        SchemaHelpers.deleteQueues(dataSource, queue)

        assertThrows<SQLException> { DatabaseConsumer(dataSource).receive(queue) }

        // The failed receive() still makes its span, and the span is marked as an error
        val spans = spanExporter.finishedSpanItems
        assertEquals(1, spans.size, spans.joinToString { "${it.kind} '${it.name}'" })
        val consumerSpan = spans.first()
        assertEquals(SpanKind.CONSUMER, consumerSpan.kind)
        assertEquals("tracing receive", consumerSpan.name)
        assertEquals(StatusCode.ERROR, consumerSpan.status.statusCode)
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
