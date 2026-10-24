package kolbasa.telemetry.tracing

import io.mockk.mockk
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.producer.Id
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.ShardId
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.time.Instant

class TracingTest {

    private val queue = Queue.of("tracing", PredefinedDataTypes.String)

    @Test
    fun testNone_Send() {
        val result = SendResult<String>(failedMessages = 0, messages = emptyList())
        var storedContext: Map<String, String>? = null
        val call = SendCall(queue, NodeId.EMPTY_NODE_ID, SendRequest(listOf(SendMessage("bugaga")))) { traceContext ->
            storedContext = traceContext
            result
        }

        // The call goes straight through and stores no trace context
        assertSame(result, Tracing.NONE.send(call))
        assertEquals(emptyMap<String, String>(), storedContext)
        assertTrue(call.proceeded)
    }

    @Test
    fun testNone_Receive() {
        val messages = listOf(message())
        val call = ReceiveCall(queue, NodeId.EMPTY_NODE_ID) { messages }

        assertSame(messages, Tracing.NONE.receive(call))
        assertTrue(call.proceeded)
    }

    @Test
    fun testProceed_OnlyOnce() {
        val sendCall = SendCall(queue, NodeId.EMPTY_NODE_ID, SendRequest(emptyList())) {
            SendResult(failedMessages = 0, messages = emptyList())
        }
        sendCall.proceed(emptyMap())
        assertThrows<IllegalStateException> { sendCall.proceed(emptyMap()) }

        val receiveCall = ReceiveCall(queue, NodeId.EMPTY_NODE_ID) { emptyList() }
        receiveCall.proceed()
        assertThrows<IllegalStateException> { receiveCall.proceed() }
    }

    @Test
    fun testProceed_NotCalled() {
        // A hook that forgets to call proceed()
        val broken = object : Tracing {
            override fun <Data> send(call: SendCall<Data>) = SendResult<Data>(failedMessages = 0, messages = emptyList())
            override fun <Data> receive(call: ReceiveCall<Data>) = emptyList<Message<Data>>()
        }
        // Nothing is sent or received, so the connection is never used
        val connection = mockk<Connection>()

        val producer = ConnectionAwareDatabaseProducer(ProducerOptions(tracing = broken))
        assertThrows<IllegalStateException> { producer.send(connection, queue, "bugaga") }

        val consumer = ConnectionAwareDatabaseConsumer(ConsumerOptions(tracing = broken))
        assertThrows<IllegalStateException> { consumer.receive(connection, queue) }
    }

    @Test
    fun testTraceContext() {
        val call = ReceiveCall(queue, NodeId.EMPTY_NODE_ID) { emptyList() }

        val withoutContext = message()
        assertEquals(emptyMap<String, String>(), call.traceContext(withoutContext))

        val withContext = message()
        withContext.traceContext = mapOf("traceparent" to "00-trace-span-01")
        assertEquals(mapOf("traceparent" to "00-trace-span-01"), call.traceContext(withContext))
    }

    private fun message() = Message(
        id = Id(1, ShardId.of(1)),
        createdAt = Instant.EPOCH,
        processingAt = Instant.EPOCH,
        scheduledAt = Instant.EPOCH,
        remainingAttempts = 1,
        data = "bugaga"
    )

}
