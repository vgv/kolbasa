package kolbasa.producer

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class SendRequestTest {

    @Test
    fun testChunked() {
        val messages = (1..100).map { index ->
            SendMessage<Int, Unit>(index)
        }

        val sendRequest = SendRequest(messages, SendOptions())
        sendRequest.openTelemetryContext = mutableListOf("1", "2", "3")

        sendRequest.chunked(10).forEachIndexed { index, chunk ->
            assertSame(sendRequest.sendOptions, chunk.sendOptions)
            assertSame(sendRequest.openTelemetryContext, chunk.openTelemetryContext)
            val expected = messages.subList(index * 10, (index + 1) * 10)
            assertEquals(expected, chunk.data)
        }
    }

    @Test
    fun testChunked_CheckOptimized() {
        val messages = (1..100).map { index ->
            SendMessage<Int, Unit>(index)
        }
        val sendOptions = SendOptions()

        val sendRequest = SendRequest(messages, sendOptions)
        sendRequest.openTelemetryContext = mutableListOf("1", "2", "3")

        // if chunkSize is bigger or equals to messages size - just return the same SendRequest
        val requestFromSequence = sendRequest.chunked(messages.size + 1).first()
        assertSame(sendRequest, requestFromSequence)
    }
}
