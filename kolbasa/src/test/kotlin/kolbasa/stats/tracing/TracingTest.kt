package kolbasa.stats.tracing

import kolbasa.consumer.Message
import kolbasa.producer.Id
import kolbasa.producer.SendMessage
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.ShardId
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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
    fun testProceeded_NotCalled() {
        val sendCall = SendCall(queue, NodeId.EMPTY_NODE_ID, SendRequest(emptyList())) {
            SendResult(failedMessages = 0, messages = emptyList())
        }
        assertFalse(sendCall.proceeded)

        val receiveCall = ReceiveCall(queue, NodeId.EMPTY_NODE_ID) { emptyList() }
        assertFalse(receiveCall.proceeded)
    }

    @Test
    fun testTraceContext() {
        val call = ReceiveCall(queue, NodeId.EMPTY_NODE_ID) { emptyList() }

        val withoutContext = message()
        assertEquals(emptyMap<String, String>(), call.traceContext(withoutContext))

        val withContext = message()
        withContext.openTelemetryData = mapOf("traceparent" to "00-trace-span-01")
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
