package kolbasa.consumer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Filter.between
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.producer.Id
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendResult.Companion.onlySuccessful
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.Searchable
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlin.test.*

class DatabaseConsumerTest : AbstractPostgresqlTest() {

    private val queue = Queue(
        "local",
        PredefinedDataTypes.String,
        metadata = TestMeta::class.java
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }

    @Test
    fun testReceive_Simple() {
        val data = "bugaga"

        val producer = DatabaseProducer(dataSource)
        val result = producer.send(queue, data)
        assertEquals(0, result.failedMessages)
        assertEquals(1, result.onlySuccessful().size)
        val id = result.onlySuccessful().first().id

        val consumer = DatabaseConsumer(dataSource)

        // Read message
        run {
            val message = consumer.receive(queue)

            assertNotNull(message)
            assertEquals(id, message.id)
            assertEquals(data, message.data)
            assertNotSame(data, message.data)
            assertTrue(message.createdAt <= message.processingAt, "message=$message")
            assertEquals(QueueOptions.DEFAULT_ATTEMPTS - 1, message.remainingAttempts)

            // delete message
            consumer.delete(queue, message)
        }

        // Try to read one more message, but queue is empty
        run {
            val message = consumer.receive(queue)
            // queue is empty
            assertNull(message)
        }
    }

    @Test
    fun testReceive_TestSimpleConcurrent() {
        val data1 = "bugaga-1"
        val data2 = "bugaga-2"

        val producer = DatabaseProducer(dataSource)
        val result1 = producer.send(queue, data1)
        val result2 = producer.send(queue, data2)
        assertEquals(0, result1.failedMessages)
        assertEquals(0, result2.failedMessages)
        assertEquals(1, result1.onlySuccessful().size)
        assertEquals(1, result2.onlySuccessful().size)
        val id1 = result1.onlySuccessful().first().id
        val id2 = result2.onlySuccessful().first().id


        val consumer = DatabaseConsumer(dataSource)

        val message1 = consumer.receive(queue)
        val message2 = consumer.receive(queue)

        // Check first message
        assertNotNull(message1)
        assertEquals(id1, message1.id)
        assertEquals(data1, message1.data)
        assertNotSame(data1, message1.data)
        assertTrue(message1.createdAt <= message1.processingAt, "message=$message1")
        assertNull(message1.meta)

        // Check second message
        assertNotNull(message2)
        assertEquals(id2, message2.id)
        assertEquals(data2, message2.data)
        assertNotSame(data2, message2.data)
        assertTrue(message2.createdAt <= message2.processingAt, "message=$message2")
        assertNull(message2.meta)
    }

    @ParameterizedTest
    @ValueSource(ints = [3, 10, 50])
    fun testReceive_TestComplexConcurrent(threads: Int) {
        val items = 5000
        val data = (1..items * threads).map {
            SendMessage<String, TestMeta>("data_$it")
        }

        val producer = DatabaseProducer(dataSource)
        val sendResult = producer.send(queue, data)

        val consumer = DatabaseConsumer(dataSource)
        val latch = CountDownLatch(1)

        val receivedIds = Collections.synchronizedSet(mutableSetOf<Id>())

        val launchedThreads = (1..threads).map { _ ->
            thread {
                // All threads have to start at the same time
                latch.await()
                val messages = consumer.receive(queue, items)

                // Check we read exactly 'items' messages
                assertEquals(items, messages.size)
                messages.forEach { message ->
                    // Check each thread read unique messages
                    assertTrue(receivedIds.add(message.id))
                }
            }
        }

        // Start all threads at the same time to imitate concurrency
        latch.countDown()
        launchedThreads.forEach(Thread::join)

        // Check we read all messages from the queue
        val sentIds = sendResult.onlySuccessful().map { it.id }.toSet()
        assertEquals(sentIds, receivedIds)
    }


    @Test
    fun testReceive_Delay() {
        val data = "bugaga"
        val delay = Duration.of(1500 + Random.nextLong(0, 1500), ChronoUnit.MILLIS)

        val producer = DatabaseProducer(dataSource)
        val id = producer.send(queue, SendMessage(data = data, messageOptions = MessageOptions(delay = delay))).let { (failedMessages, result) ->
            assertEquals(0, failedMessages)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }


        val consumer = DatabaseConsumer(dataSource)

        var message: Message<String, TestMeta>? = consumer.receive(queue)
        while (message == null) {
            TimeUnit.MILLISECONDS.sleep(10)
            message = consumer.receive(queue)
        }

        // Check delay
        assertTrue(
            (message.processingAt - message.createdAt) >= delay.toMillis(),
            "Message: $message, delay=$delay"
        )

        // Check message
        assertNotNull(message)
        assertEquals(id, message.id)
        assertEquals(data, message.data)
        assertNotSame(data, message.data)
        assertTrue(message.createdAt <= message.processingAt, "message=$message")
        assertNull(message.meta)
    }

    @Test
    fun testReceive_VisibitilyTimeout() {
        val data = "bugaga"

        val producer = DatabaseProducer(dataSource)
        val result = producer.send(queue, data)
        assertEquals(0, result.failedMessages)
        assertEquals(1, result.onlySuccessful().size)
        val id = result.onlySuccessful().first().id

        val consumer = DatabaseConsumer(dataSource)

        val delay = Duration.of(1500 + Random.nextLong(0, 1000), ChronoUnit.MILLIS)
        val receiveOptions = ReceiveOptions<TestMeta>(visibilityTimeout = delay)

        // Read a message first time
        val firstMessage = consumer.receive(queue, receiveOptions)

        // Check it
        assertNotNull(firstMessage)
        assertEquals(id, firstMessage.id)
        assertEquals(data, firstMessage.data)
        assertNotSame(data, firstMessage.data)
        assertTrue(firstMessage.createdAt <= firstMessage.processingAt, "message=$firstMessage")
        assertEquals(QueueOptions.DEFAULT_ATTEMPTS - 1, firstMessage.remainingAttempts)
        assertNull(firstMessage.meta)

        // Try to read this message again
        var secondMessage: Message<String, TestMeta>? = consumer.receive(queue, receiveOptions)
        while (secondMessage == null) {
            TimeUnit.MILLISECONDS.sleep(10)
            secondMessage = consumer.receive(queue, receiveOptions)
        }

        // Check that second message has the same ID and DATA, but not the same object
        assertNotSame(firstMessage, secondMessage)
        assertEquals(id, secondMessage.id)
        assertEquals(data, secondMessage.data)
        assertNotSame(data, secondMessage.data)
        assertTrue(secondMessage.createdAt <= secondMessage.processingAt, "message=$secondMessage")
        assertEquals(QueueOptions.DEFAULT_ATTEMPTS - 2, secondMessage.remainingAttempts)
        assertNull(secondMessage.meta)

        // The second message was read later, so, this condition has to be true
        assertTrue(
            firstMessage.processingAt < secondMessage.processingAt,
            "First: $firstMessage, second: $secondMessage"
        )

        // Check that interval between message became available is not less than visibilityTimeout delay
        assertTrue(
            (secondMessage.processingAt - firstMessage.processingAt) >= delay.toMillis(),
            "First: ${firstMessage.processingAt}, second: ${secondMessage.processingAt}, delay=$delay"
        )
    }

    @Test
    fun testReceive_ReadMetadata() {
        val data = "bugaga"

        val producer = DatabaseProducer(dataSource)
        val (failedMessages1, result1) = producer.send(queue, SendMessage(data, TestMeta(1)))
        val (failedMessages2, result2) = producer.send(queue, SendMessage(data, TestMeta(2)))
        assertEquals(0, failedMessages1)
        assertEquals(0, failedMessages2)
        assertEquals(1, result1.onlySuccessful().size)
        assertEquals(1, result2.onlySuccessful().size)
        val id1 = result1.onlySuccessful().first().id
        val id2 = result2.onlySuccessful().first().id


        val consumer = DatabaseConsumer(dataSource)

        // Read first message without metadata and check it
        val withoutMetadata = consumer.receive(queue, ReceiveOptions(readMetadata = false))
        assertNotNull(withoutMetadata)
        assertNull(withoutMetadata.meta)
        assertEquals(id1, withoutMetadata.id)
        assertEquals(data, withoutMetadata.data)
        assertNotSame(data, withoutMetadata.data)
        assertTrue(withoutMetadata.createdAt <= withoutMetadata.processingAt, "message=$withoutMetadata")

        // Read second message with metadata and check it
        val withMetadata = consumer.receive(queue, ReceiveOptions(readMetadata = true))
        assertNotNull(withMetadata)
        assertNotNull(withMetadata.meta) {
            assertEquals(2, it.field)
        }
        assertEquals(id2, withMetadata.id)
        assertEquals(data, withMetadata.data)
        assertNotSame(data, withMetadata.data)
        assertTrue(withMetadata.createdAt <= withMetadata.processingAt, "message=$withMetadata")
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testReceive_Order(readMetadata: Boolean) {
        val items = 100
        val data = (1..items).map { index ->
            SendMessage("bugaga_$index", TestMeta(index))
        }

        val producer = DatabaseProducer(dataSource)
        producer.send(queue, data)

        val consumer = DatabaseConsumer(dataSource)
        // messages were sent with natural ordering, TestMeta.field is 1,2,3,4...
        // Try to read them in reverse order
        val messages = consumer.receive(
            queue,
            limit = items,
            receiveOptions = ReceiveOptions(
                readMetadata = readMetadata,
                order = TestMeta::field.desc()
            )
        )

        // Check reverse ordering
        var reverseIndex = items
        messages.forEach { message ->
            assertNotNull(message)
            assertEquals("bugaga_$reverseIndex", message.data)
            assertTrue(message.createdAt <= message.processingAt, "message=$message")
            if (readMetadata) {
                assertNotNull(message.meta) {
                    assertEquals(reverseIndex, it.field)
                }
            } else {
                assertNull(message.meta)
            }

            reverseIndex--
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testReceive_Filter(readMetadata: Boolean) {
        val items = 100
        val data = (1..items).map { index ->
            SendMessage("bugaga_$index", TestMeta(index))
        }

        val producer = DatabaseProducer(dataSource)
        producer.send(queue, data)

        val consumer = DatabaseConsumer(dataSource)
        // messages have TestMeta.field from 1 till 100
        // Try to read only messages from 10 till 15
        val start = 10
        val end = 15
        val messages = consumer.receive(
            queue,
            limit = items,
            receiveOptions = ReceiveOptions(
                readMetadata = readMetadata,
                filter = TestMeta::field between Pair(start, end),
                order = TestMeta::field.desc() // add order just to simplify testing
            )
        )

        // Check filtering
        var index = end
        assertEquals(end - start + 1, messages.size)
        messages.forEach { message ->
            assertNotNull(message)
            assertEquals("bugaga_$index", message.data)
            assertTrue(message.createdAt <= message.processingAt, "message=$message")
            if (readMetadata) {
                assertNotNull(message.meta) {
                    assertEquals(index, it.field)
                }
            } else {
                assertNull(message.meta)
            }

            index--
        }
    }

}

internal data class TestMeta(@Searchable val field: Int)
