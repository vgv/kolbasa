package kolbasa.producer.datasource

import kolbasa.producer.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class ProducerInterceptorTest {

    @Test
    fun testDefaultInterfaceMethods() {
        // Check that default interface methods really don't change anything
        val defaultImpl = object : ProducerInterceptor<String, Unit> {}

        val originalRequest = SendRequest<String, Unit>(emptyList())
        val originalResult = SendResult<String, Unit>(failedMessages = 0, messages = emptyList())

        assertSame(originalRequest, defaultImpl.beforeSend(originalRequest))
        assertSame(originalResult, defaultImpl.aroundSend(originalRequest) { req ->
            assertSame(originalRequest, req)
            originalResult
        })
        assertSame(originalResult, defaultImpl.afterSend(originalResult))
    }

    @Test
    fun testRecursiveApply() {
        val interceptors = listOf(
            TestInterceptor(1),
            TestInterceptor(2),
            TestInterceptor(3),
        )

        val originalRequest = SendRequest<String, Unit>(emptyList())
        val originalResult = SendResult<String, Unit>(failedMessages = 0, messages = emptyList())

        val finalResult = ProducerInterceptor.recursiveApplyInterceptors(
            interceptors,
            originalRequest
        ) { req ->
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
    ) : ProducerInterceptor<String, Unit> {

        override fun beforeSend(request: SendRequest<String, Unit>): SendRequest<String, Unit> {
            return request.copy(data = request.data + SendMessage("before_$id"))
        }

        override fun aroundSend(
            request: SendRequest<String, Unit>,
            call: (SendRequest<String, Unit>) -> SendResult<String, Unit>
        ): SendResult<String, Unit> {
            val newRequest = request.copy(data = request.data + SendMessage("around_$id"))
            val result = call(newRequest)
            return result.copy(
                failedMessages = result.failedMessages,
                messages = result.messages + MessageResult.Success(Id(id, null), SendMessage("around_$id"))
            )
        }

        override fun afterSend(result: SendResult<String, Unit>): SendResult<String, Unit> {
            return result.copy(
                failedMessages = result.failedMessages,
                messages = result.messages + MessageResult.Success(Id(id, null), SendMessage("after_$id"))
            )
        }
    }

}
