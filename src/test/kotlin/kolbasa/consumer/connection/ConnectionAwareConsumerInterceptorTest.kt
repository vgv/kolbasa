package kolbasa.consumer.connection

import io.mockk.mockk
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.Id
import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertSame

class ConnectionAwareConsumerInterceptorTest {

    @Test
    fun testDefaultInterfaceMethods() {
        // Check that default interface methods really don't change anything
        val defaultImpl = object : ConnectionAwareConsumerInterceptor<String, Unit> {}

        // Receive
        val connection = mockk<Connection>()
        val originalLimit = 123
        val originalOptions = ReceiveOptions<Unit>()
        val originalResult = listOf<Message<String, Unit>>()

        assertSame(originalOptions, defaultImpl.beforeReceive(connection, originalLimit, originalOptions))
        assertSame(originalResult, defaultImpl.aroundReceive(connection, originalLimit, originalOptions) { conn, lmt, rcvOptions ->
            assertSame(connection, conn)
            assertEquals(originalLimit, lmt)
            assertSame(originalOptions, rcvOptions)

            originalResult
        })
        assertSame(originalResult, defaultImpl.afterReceive(connection, originalResult))

        // Delete
        val originalRequest = listOf<Id>()
        val originalResponse = 1313
        assertSame(originalRequest, defaultImpl.beforeDelete(connection, originalRequest))
        assertEquals(originalResponse, defaultImpl.aroundDelete(connection, originalRequest) { conn, req ->
            assertSame(connection, conn)
            assertSame(originalRequest, req)
            originalResponse
        })
        assertEquals(originalResponse, defaultImpl.afterDelete(connection, originalResponse))
    }

    @Test
    fun testRecursiveApplyReceiveInterceptors() {
        val requestCapture = mutableListOf<ReceiveOptions<Unit>>()
        val responseCapture = mutableListOf<List<Message<String, Unit>>>()

        val firstBeforeRequest = ReceiveOptions<Unit>()
        val firstAroundRequest = ReceiveOptions<Unit>()
        val firstAroundResult = listOf<Message<String, Unit>>()
        val firstAfterResult = listOf<Message<String, Unit>>()

        val secondBeforeRequest = ReceiveOptions<Unit>()
        val secondAroundRequest = ReceiveOptions<Unit>()
        val secondAroundResult = listOf<Message<String, Unit>>()
        val secondAfterResult = listOf<Message<String, Unit>>()

        val thirdBeforeRequest = ReceiveOptions<Unit>()
        val thirdAroundRequest = ReceiveOptions<Unit>()
        val thirdAroundResult = listOf<Message<String, Unit>>()
        val thirdAfterResult = listOf<Message<String, Unit>>()


        val interceptors = listOf(
            TestInterceptorForReceive(
                requestCapture,
                responseCapture,
                firstBeforeRequest,
                firstAroundRequest,
                firstAroundResult,
                firstAfterResult
            ),
            TestInterceptorForReceive(
                requestCapture,
                responseCapture,
                secondBeforeRequest,
                secondAroundRequest,
                secondAroundResult,
                secondAfterResult
            ),
            TestInterceptorForReceive(
                requestCapture,
                responseCapture,
                thirdBeforeRequest,
                thirdAroundRequest,
                thirdAroundResult,
                thirdAfterResult
            )
        )

        val connection = mockk<Connection>()
        val originalLimit = 123
        val originalRequest = ReceiveOptions<Unit>()
        val originalResult = listOf<Message<String, Unit>>()

        val finalResult = ConnectionAwareConsumerInterceptor.recursiveApplyReceiveInterceptors(
            interceptors,
            connection,
            originalLimit,
            originalRequest
        ) { conn, lmt, req ->
            assertSame(connection, conn)
            assertEquals(originalLimit, lmt)
            assertSame(thirdAroundRequest, req)

            originalResult
        }


        // check interceptors nesting
        assertEquals(6, requestCapture.size)
        assertSame(originalRequest, requestCapture[0])
        assertSame(firstBeforeRequest, requestCapture[1])
        assertSame(firstAroundRequest, requestCapture[2])
        assertSame(secondBeforeRequest, requestCapture[3])
        assertSame(secondAroundRequest, requestCapture[4])
        assertSame(thirdBeforeRequest, requestCapture[5])

        assertEquals(6, responseCapture.size)
        assertSame(originalResult, responseCapture[0])
        assertSame(thirdAroundResult, responseCapture[1])
        assertSame(thirdAfterResult, responseCapture[2])
        assertSame(secondAroundResult, responseCapture[3])
        assertSame(secondAfterResult, responseCapture[4])
        assertSame(firstAroundResult, responseCapture[5])
        assertSame(finalResult, firstAfterResult)
    }

    @Test
    fun testRecursiveApplyDeleteInterceptors() {
        val requestCapture = mutableListOf<List<Id>>()
        val responseCapture = mutableListOf<Int>()

        val firstBeforeRequest = listOf<Id>()
        val firstAroundRequest = listOf<Id>()
        val firstAroundResult = 423423
        val firstAfterResult = 56423

        val secondBeforeRequest = listOf<Id>()
        val secondAroundRequest = listOf<Id>()
        val secondAroundResult = 63452
        val secondAfterResult = 879645

        val thirdBeforeRequest = listOf<Id>()
        val thirdAroundRequest = listOf<Id>()
        val thirdAroundResult = 1234334
        val thirdAfterResult = 345567


        val interceptors = listOf(
            TestInterceptorForDelete(
                requestCapture,
                responseCapture,
                firstBeforeRequest,
                firstAroundRequest,
                firstAroundResult,
                firstAfterResult
            ),
            TestInterceptorForDelete(
                requestCapture,
                responseCapture,
                secondBeforeRequest,
                secondAroundRequest,
                secondAroundResult,
                secondAfterResult
            ),
            TestInterceptorForDelete(
                requestCapture,
                responseCapture,
                thirdBeforeRequest,
                thirdAroundRequest,
                thirdAroundResult,
                thirdAfterResult
            )
        )

        val connection = mockk<Connection>()
        val originalRequest = listOf<Id>()
        val originalResult = 234564

        val finalResult = ConnectionAwareConsumerInterceptor.recursiveApplyDeleteInterceptors(
            interceptors,
            connection,
            originalRequest
        ) { conn, req ->
            assertSame(connection, conn)
            assertSame(thirdAroundRequest, req)

            originalResult
        }


        // check interceptors nesting
        assertEquals(6, requestCapture.size)
        assertSame(originalRequest, requestCapture[0])
        assertSame(firstBeforeRequest, requestCapture[1])
        assertSame(firstAroundRequest, requestCapture[2])
        assertSame(secondBeforeRequest, requestCapture[3])
        assertSame(secondAroundRequest, requestCapture[4])
        assertSame(thirdBeforeRequest, requestCapture[5])

        assertEquals(6, responseCapture.size)
        assertEquals(originalResult, responseCapture[0])
        assertEquals(thirdAroundResult, responseCapture[1])
        assertEquals(thirdAfterResult, responseCapture[2])
        assertEquals(secondAroundResult, responseCapture[3])
        assertEquals(secondAfterResult, responseCapture[4])
        assertEquals(firstAroundResult, responseCapture[5])
        assertEquals(finalResult, firstAfterResult)
    }

    private class TestInterceptorForReceive(
        private val requestCapture: MutableList<ReceiveOptions<Unit>>,
        private val responseCapture: MutableList<List<Message<String, Unit>>>,

        private val beforeRequest: ReceiveOptions<Unit>,
        private val aroundRequest: ReceiveOptions<Unit>,
        private val aroundResult: List<Message<String, Unit>>,
        private val afterResult: List<Message<String, Unit>>
    ) : ConnectionAwareConsumerInterceptor<String, Unit> {

        override fun beforeReceive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<Unit>): ReceiveOptions<Unit> {
            requestCapture += receiveOptions
            return beforeRequest
        }

        override fun aroundReceive(
            connection: Connection,
            limit: Int,
            receiveOptions: ReceiveOptions<Unit>,
            call: (Connection, Int, ReceiveOptions<Unit>) -> List<Message<String, Unit>>
        ): List<Message<String, Unit>> {
            requestCapture += receiveOptions
            responseCapture += call(connection, limit, aroundRequest)
            return aroundResult
        }

        override fun afterReceive(connection: Connection, result: List<Message<String, Unit>>): List<Message<String, Unit>> {
            responseCapture += result
            return afterResult
        }
    }

    private class TestInterceptorForDelete(
        private val requestCapture: MutableList<List<Id>>,
        private val responseCapture: MutableList<Int>,

        private val beforeRequest: List<Id>,
        private val aroundRequest: List<Id>,
        private val aroundResult: Int,
        private val afterResult: Int
    ) : ConnectionAwareConsumerInterceptor<String, Unit> {

        override fun beforeDelete(connection: Connection, messageIds: List<Id>): List<Id> {
            requestCapture += messageIds
            return beforeRequest
        }

        override fun aroundDelete(connection: Connection, messageIds: List<Id>, call: (Connection, List<Id>) -> Int): Int {
            requestCapture += messageIds
            responseCapture += call(connection, aroundRequest)
            return aroundResult
        }

        override fun afterDelete(connection: Connection, result: Int): Int {
            responseCapture += result
            return afterResult
        }
    }

}
