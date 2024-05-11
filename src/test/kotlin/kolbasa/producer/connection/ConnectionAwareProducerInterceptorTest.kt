package kolbasa.producer.connection

import io.mockk.mockk
import kolbasa.producer.MessageResult
import kolbasa.producer.SendMessage
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertSame

class ConnectionAwareProducerInterceptorTest {

    @Test
    fun testDefaultInterfaceMethods() {
        // Check that default interface methods really don't change anything
        val defaultImpl = object : ConnectionAwareProducerInterceptor<String, Unit> {}

        val connection = mockk<Connection>()
        val originalRequest = SendRequest<String, Unit>(emptyList())
        val originalResult = SendResult<String, Unit>(failedMessages = 0, messages = emptyList())

        assertSame(originalRequest, defaultImpl.beforeSend(connection, originalRequest))
        assertSame(originalResult, defaultImpl.aroundSend(connection, originalRequest) { _, req ->
            assertSame(originalRequest, req)
            originalResult
        })
        assertSame(originalResult, defaultImpl.afterSend(connection, originalResult))
    }

    @Test
    fun testRecursiveApply() {
        val interceptors = listOf(
            TestInterceptor(1),
            TestInterceptor(2),
            TestInterceptor(3),
        )

        val connection = mockk<Connection>()
        val originalRequest = SendRequest<String, Unit>(emptyList())
        val originalResult = SendResult<String, Unit>(failedMessages = 0, messages = emptyList())

        val finalResult = ConnectionAwareProducerInterceptor.recursiveApplyInterceptors(
            interceptors,
            connection,
            originalRequest
        ) { _, req ->
            // check request modification
            assertEquals(6, req.data.size)
            assertEquals("before_1", req.data[0].data)
            assertEquals("around_1", req.data[1].data)
            assertEquals("before_2", req.data[2].data)
            assertEquals("around_2", req.data[3].data)
            assertEquals("before_3", req.data[4].data)
            assertEquals("around_3", req.data[5].data)

            originalResult
        }

        // check interceptors nesting
        assertEquals(6, finalResult.messages.size)
        assertEquals("around_3", (finalResult.messages[0] as MessageResult.Success).message.data)
        assertEquals("after_3", (finalResult.messages[1] as MessageResult.Success).message.data)
        assertEquals("around_2", (finalResult.messages[2] as MessageResult.Success).message.data)
        assertEquals("after_2", (finalResult.messages[3] as MessageResult.Success).message.data)
        assertEquals("around_1", (finalResult.messages[4] as MessageResult.Success).message.data)
        assertEquals("after_1", (finalResult.messages[5] as MessageResult.Success).message.data)
    }

    private class TestInterceptor(
        private val id: Long
    ) : ConnectionAwareProducerInterceptor<String, Unit> {

        override fun beforeSend(connection: Connection, request: SendRequest<String, Unit>): SendRequest<String, Unit> {
            return request.copy(data = request.data + SendMessage("before_$id"))
        }

        override fun aroundSend(
            connection: Connection,
            request: SendRequest<String, Unit>,
            call: (Connection, SendRequest<String, Unit>) -> SendResult<String, Unit>
        ): SendResult<String, Unit> {
            val newRequest = request.copy(data = request.data + SendMessage("around_$id"))
            val result = call(connection, newRequest)
            return result.copy(
                failedMessages = result.failedMessages,
                messages = result.messages + MessageResult.Success(id, SendMessage("around_$id"))
            )
        }

        override fun afterSend(connection: Connection, result: SendResult<String, Unit>): SendResult<String, Unit> {
            return result.copy(
                failedMessages = result.failedMessages,
                messages = result.messages + MessageResult.Success(id, SendMessage("after_$id"))
            )
        }
    }

}
