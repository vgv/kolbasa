package kolbasa.consumer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.assertNotNull
import kolbasa.assertTrue
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Filter.between
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendResult.Companion.onlySuccessful
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.math.abs
import kotlin.random.Random

class DatabaseConsumerTest : AbstractPostgresqlTest() {

    private val FIELD = MetaField.int("field", FieldOption.SEARCH)

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIELD)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
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
            assertTrue("message=$message") {
                val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
                compareTimestamps(message.processingAt + visibilityTimeoutMillis, message.scheduledAt)
            }
            assertEquals(QueueOptions.DEFAULT.defaultAttempts - 1, message.remainingAttempts)

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
        assertTrue("message=$message1") {
            val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
            compareTimestamps(message1.processingAt + visibilityTimeoutMillis, message1.scheduledAt)
        }
        assertSame(MetaValues.EMPTY, message1.meta)

        // Check second message
        assertNotNull(message2)
        assertEquals(id2, message2.id)
        assertEquals(data2, message2.data)
        assertNotSame(data2, message2.data)
        assertTrue(message2.createdAt <= message2.processingAt, "message=$message2")
        assertTrue("message=$message2") {
            val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
            compareTimestamps(message2.processingAt + visibilityTimeoutMillis, message2.scheduledAt)
        }
        assertSame(MetaValues.EMPTY, message2.meta)
    }

    @ParameterizedTest
    @ValueSource(ints = [3, 10, 50])
    fun testReceive_TestComplexConcurrent(threads: Int) {
        val items = 5000
        val data = (1..items * threads).map {
            SendMessage("data_$it")
        }

        val producer = DatabaseProducer(dataSource)
        val sendResult = producer.send(queue, data)

        val consumer = DatabaseConsumer(dataSource)
        val latch = CountDownLatch(1)

        val receivedMessages = Collections.synchronizedList(arrayListOf<Message<String>>())

        val launchedThreads = (1..threads).map { _ ->
            thread {
                // All threads have to start at the same time
                latch.await()
                val messages = consumer.receive(queue, items)

                // Check we read exactly 'items' messages
                assertEquals(items, messages.size)
                receivedMessages += messages
            }
        }

        // Start all threads at the same time to imitate concurrency
        latch.countDown()
        // Wait for all threads to finish
        launchedThreads.forEach(Thread::join)

        // Check we read all messages from the queue and, next, check every message individually
        val sentIds = sendResult.onlySuccessful().map { it.id }.toSet()
        val receivedIds = receivedMessages.map { it.id }.toSet()
        assertEquals(sentIds, receivedIds)

        // Check every message
        receivedMessages.forEach { message ->
            assertTrue(message.createdAt <= message.processingAt, "message=$message")
            assertTrue("message=$message") {
                val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
                compareTimestamps(message.processingAt + visibilityTimeoutMillis, message.scheduledAt)
            }
            assertEquals(QueueOptions.DEFAULT.defaultAttempts - 1, message.remainingAttempts)
        }
    }


    @Test
    fun testReceive_Delay() {
        val data = "bugaga"
        val delay = Duration.of(1500 + Random.nextLong(0, 1500), ChronoUnit.MILLIS)

        val producer = DatabaseProducer(dataSource)
        val id = producer.send(queue, SendMessage(data = data, messageOptions = MessageOptions(delay = delay)))
            .let { (failedMessages, result) ->
                assertEquals(0, failedMessages)
                assertEquals(1, result.onlySuccessful().size)
                result.onlySuccessful().first().id
            }


        val consumer = DatabaseConsumer(dataSource)

        var message: Message<String>? = consumer.receive(queue)
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
        assertTrue("message=$message") {
            val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
            compareTimestamps(message.processingAt + visibilityTimeoutMillis, message.scheduledAt)
        }

        assertSame(MetaValues.EMPTY, message.meta)
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
        val receiveOptions = ReceiveOptions(visibilityTimeout = delay)

        // Read a message first time
        val firstMessage = consumer.receive(queue, receiveOptions)

        // Check it
        assertNotNull(firstMessage)
        assertEquals(id, firstMessage.id)
        assertEquals(data, firstMessage.data)
        assertNotSame(data, firstMessage.data)
        assertTrue(firstMessage.createdAt <= firstMessage.processingAt, "message=$firstMessage")
        assertTrue("message=$firstMessage") {
            val visibilityTimeoutMillis = delay.toMillis()
            compareTimestamps(firstMessage.processingAt + visibilityTimeoutMillis, firstMessage.scheduledAt)
        }
        assertEquals(QueueOptions.DEFAULT.defaultAttempts - 1, firstMessage.remainingAttempts)
        assertSame(MetaValues.EMPTY, firstMessage.meta)

        // Try to read this message again
        var secondMessage: Message<String>? = consumer.receive(queue, receiveOptions)
        while (secondMessage == null) {
            TimeUnit.MILLISECONDS.sleep(1)
            secondMessage = consumer.receive(queue, receiveOptions)
        }

        // Check that second message has the same ID and DATA, but not the same object
        assertNotSame(firstMessage, secondMessage)
        assertEquals(id, secondMessage.id)
        assertEquals(data, secondMessage.data)
        assertNotSame(data, secondMessage.data)
        assertTrue(secondMessage.createdAt <= secondMessage.processingAt, "message=$secondMessage")
        assertTrue("message=$secondMessage") {
            val visibilityTimeoutMillis = delay.toMillis()
            compareTimestamps(secondMessage.processingAt + visibilityTimeoutMillis, secondMessage.scheduledAt)
        }
        assertEquals(QueueOptions.DEFAULT.defaultAttempts - 2, secondMessage.remainingAttempts)
        assertSame(MetaValues.EMPTY, secondMessage.meta)

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
    fun testReceive_CheckDurationBiggerThanMaxInt() {
        val data = "bugaga"

        val producer = DatabaseProducer(dataSource)
        val result = producer.send(queue, data)
        assertEquals(0, result.failedMessages)
        assertEquals(1, result.onlySuccessful().size)
        val id = result.onlySuccessful().first().id

        val consumer = DatabaseConsumer(dataSource)

        val delay = Duration.ofHours(24 * 365 * 1000) // approx. 1000 years
        val receiveOptions = ReceiveOptions(visibilityTimeout = delay)

        // Read a message, it will update scheduled_at field
        val firstMessage = consumer.receive(queue, receiveOptions)

        // Check it
        assertNotNull(firstMessage)
        assertEquals(id, firstMessage.id)
        assertEquals(data, firstMessage.data)
        assertNotSame(data, firstMessage.data)
        assertTrue(firstMessage.createdAt <= firstMessage.processingAt, "message=$firstMessage")



        assertTrue("message=$firstMessage") {
            val visibilityTimeoutMillis = delay.toMillis()
            compareTimestamps(firstMessage.processingAt + visibilityTimeoutMillis, firstMessage.scheduledAt)
        }



        assertEquals(QueueOptions.DEFAULT.defaultAttempts - 1, firstMessage.remainingAttempts)
        assertSame(MetaValues.EMPTY, firstMessage.meta)
    }

    @Test
    fun testReceive_ReadMetadata() {
        val data = "bugaga"

        val producer = DatabaseProducer(dataSource)
        val (failedMessages1, result1) = producer.send(queue, SendMessage(data, MetaValues.of(FIELD.value(1))))
        val (failedMessages2, result2) = producer.send(queue, SendMessage(data, MetaValues.of(FIELD.value(2))))
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
        assertSame(MetaValues.EMPTY, withoutMetadata.meta)
        assertEquals(id1, withoutMetadata.id)
        assertEquals(data, withoutMetadata.data)
        assertNotSame(data, withoutMetadata.data)
        assertTrue(withoutMetadata.createdAt <= withoutMetadata.processingAt, "message=$withoutMetadata")
        assertTrue("message=$withoutMetadata") {
            val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
            compareTimestamps(withoutMetadata.processingAt + visibilityTimeoutMillis, withoutMetadata.scheduledAt)
        }


        // Read second message with metadata and check it
        val withMetadata = consumer.receive(queue, ReceiveOptions(readMetadata = true))
        assertNotNull(withMetadata)
        assertEquals(2, withMetadata.meta.get(FIELD))
        assertEquals(id2, withMetadata.id)
        assertEquals(data, withMetadata.data)
        assertNotSame(data, withMetadata.data)
        assertTrue(withMetadata.createdAt <= withMetadata.processingAt, "message=$withMetadata")
        assertTrue("message=$withMetadata") {
            val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
            compareTimestamps(withMetadata.processingAt + visibilityTimeoutMillis, withMetadata.scheduledAt)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testReceive_Order(readMetadata: Boolean) {
        val items = 100
        val data = (1..items).map { index ->
            SendMessage("bugaga_$index", MetaValues.of(FIELD.value(index)))
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
                order = FIELD.desc()
            )
        )

        // Check reverse ordering
        var reverseIndex = items
        messages.forEach { message ->
            assertNotNull(message)
            assertEquals("bugaga_$reverseIndex", message.data)
            assertTrue(message.createdAt <= message.processingAt, "message=$message")
            assertTrue("message=$message") {
                val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
                compareTimestamps(message.processingAt + visibilityTimeoutMillis, message.scheduledAt)
            }
            if (readMetadata) {
                assertEquals(reverseIndex, message.meta.get(FIELD))
            } else {
                assertSame(MetaValues.EMPTY, message.meta)
            }

            reverseIndex--
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testReceive_Filter(readMetadata: Boolean) {
        val items = 100
        val data = (1..items).map { index ->
            SendMessage("bugaga_$index", MetaValues.of(FIELD.value(index)))
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
                filter = FIELD between Pair(start, end),
                order = FIELD.desc() // add order just to simplify testing
            )
        )

        // Check filtering
        var index = end
        assertEquals(end - start + 1, messages.size)
        messages.forEach { message ->
            assertNotNull(message)
            assertEquals("bugaga_$index", message.data)
            assertTrue(message.createdAt <= message.processingAt, "message=$message")
            assertTrue("message=$message") {
                val visibilityTimeoutMillis = QueueOptions.DEFAULT.defaultVisibilityTimeout.toMillis()
                compareTimestamps(message.processingAt + visibilityTimeoutMillis, message.scheduledAt)
            }
            if (readMetadata) {
                assertEquals(index, message.meta.get(FIELD))
            } else {
                assertSame(MetaValues.EMPTY, message.meta)
            }

            index--
        }
    }

    private fun compareTimestamps(first: Long, second: Long, delta: Duration = Duration.ofMillis(50)): Boolean {
        val diff = abs(first - second)
        return diff < delta.toMillis()
    }

}
