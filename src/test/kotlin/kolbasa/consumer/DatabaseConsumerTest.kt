package kolbasa.consumer

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.filter.Filter.between
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.producer.DatabaseProducer
import kolbasa.producer.SendMessage
import kolbasa.producer.MessageOptions
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
import java.util.concurrent.ConcurrentSkipListSet
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

        val producer = DatabaseProducer(dataSource, queue)
        val id = producer.send(data)

        val consumer = DatabaseConsumer(dataSource, queue)

        // Read message
        run {
            val message = consumer.receive()

            assertNotNull(message)
            assertEquals(id, message.id)
            assertEquals(data, message.data)
            assertNotSame(data, message.data)
            assertTrue(message.createdAt < message.processingAt, "message=$message")
            assertEquals(QueueOptions.DEFAULT_ATTEMPTS - 1, message.remainingAttempts)

            // delete message
            consumer.delete(message)
        }

        // Try to read one more message, but queue is empty
        run {
            val message = consumer.receive()
            // queue is empty
            assertNull(message)
        }
    }

    @Test
    fun testReceive_TestSimpleConcurrent() {
        val data1 = "bugaga-1"
        val data2 = "bugaga-2"

        val producer = DatabaseProducer(dataSource, queue)
        val id1 = producer.send(data1)
        val id2 = producer.send(data2)

        val consumer = DatabaseConsumer(dataSource, queue)

        val message1 = consumer.receive()
        val message2 = consumer.receive()

        // Check first message
        assertNotNull(message1)
        assertEquals(id1, message1.id)
        assertEquals(data1, message1.data)
        assertNotSame(data1, message1.data)
        assertTrue(message1.createdAt < message1.processingAt)
        assertNull(message1.meta)

        // Check second message
        assertNotNull(message2)
        assertEquals(id2, message2.id)
        assertEquals(data2, message2.data)
        assertNotSame(data2, message2.data)
        assertTrue(message2.createdAt < message2.processingAt)
        assertNull(message2.meta)
    }

    @ParameterizedTest
    @ValueSource(ints = [3, 10, 50])
    fun testReceive_TestComplexConcurrent(threads: Int) {
        val items = 5000
        val data = (1..items * threads).map {
            SendMessage<String, TestMeta>("data_$it")
        }

        val producer = DatabaseProducer(dataSource, queue)
        val sendResult = producer.send(data)

        val consumer = DatabaseConsumer(dataSource, queue)
        val latch = CountDownLatch(1)

        val receivedIds = ConcurrentSkipListSet<Long>()

        val launchedThreads = (1..threads).map { _ ->
            thread {
                // All threads have to start at the same time
                latch.await()
                val messages = consumer.receive(items)

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
        val delay = Duration.of(3000 + Random.nextLong(0, 2000), ChronoUnit.MILLIS)

        val producer = DatabaseProducer(dataSource, queue)
        val id = producer.send(SendMessage(data = data, messageOptions = MessageOptions(delay = delay)))
        val sendTimestamp = System.nanoTime()

        val consumer = DatabaseConsumer(dataSource, queue)

        var message: Message<String, TestMeta>?
        var receiveTimestamp: Long
        do {
            message = consumer.receive()
            receiveTimestamp = System.nanoTime()
            TimeUnit.MILLISECONDS.sleep(50)
        } while (message == null)

        // Check delay
        assertTrue(
            (receiveTimestamp - sendTimestamp) > delay.toNanos(),
            "Receive: $receiveTimestamp, send: $sendTimestamp, delay=$delay"
        )

        // Check message
        assertNotNull(message)
        assertEquals(id, message.id)
        assertEquals(data, message.data)
        assertNotSame(data, message.data)
        assertTrue(message.createdAt < message.processingAt, "message=$message")
        assertNull(message.meta)
    }

    @Test
    fun testReceive_VisibitilyTimeout() {
        val data = "bugaga"

        val producer = DatabaseProducer(dataSource, queue)
        val id = producer.send(data)

        val consumer = DatabaseConsumer(dataSource, queue)

        val delay = Duration.of(3000 + Random.nextLong(0, 2000), ChronoUnit.MILLIS)
        val receiveOptions = ReceiveOptions<TestMeta>(visibilityTimeout = delay)

        // Read a message first time
        val firstMessage = consumer.receive(receiveOptions)
        val firstReceiveTimestamp = System.nanoTime()

        // Check it
        assertNotNull(firstMessage)
        assertEquals(id, firstMessage.id)
        assertEquals(data, firstMessage.data)
        assertNotSame(data, firstMessage.data)
        assertTrue(firstMessage.createdAt < firstMessage.processingAt, "message=$firstMessage")
        assertEquals(QueueOptions.DEFAULT_ATTEMPTS - 1, firstMessage.remainingAttempts)
        assertNull(firstMessage.meta)

        // Try to read this message again
        var secondMessage: Message<String, TestMeta>?
        var secondReceiveTimestamp: Long
        do {
            secondMessage = consumer.receive(receiveOptions)
            secondReceiveTimestamp = System.nanoTime()
            TimeUnit.MILLISECONDS.sleep(50)
        } while (secondMessage == null)

        // Check that second message has the same ID and DATA, but not the same object
        assertNotSame(firstMessage, secondMessage)
        assertEquals(id, secondMessage.id)
        assertEquals(data, secondMessage.data)
        assertNotSame(data, secondMessage.data)
        assertTrue(secondMessage.createdAt < secondMessage.processingAt, "message=$secondMessage")
        assertEquals(QueueOptions.DEFAULT_ATTEMPTS - 2, secondMessage.remainingAttempts)
        assertNull(secondMessage.meta)

        // The second message was read later, so, this condition has to be true
        assertTrue(
            firstMessage.processingAt < secondMessage.processingAt,
            "First: $firstMessage, second: $secondMessage"
        )

        // Check that interval between message became available is not less than visibilityTimeout delay
        assertTrue(
            (secondReceiveTimestamp - firstReceiveTimestamp) > delay.toNanos(),
            "First: $firstReceiveTimestamp, second: $secondReceiveTimestamp, delay=$delay"
        )
    }

    @Test
    fun testReceive_ReadMetadata() {
        val data = "bugaga"

        val producer = DatabaseProducer(dataSource, queue)
        val id1 = producer.send(SendMessage(data, TestMeta(1)))
        val id2 = producer.send(SendMessage(data, TestMeta(2)))

        val consumer = DatabaseConsumer(dataSource, queue)

        // Read first message without metadata and check it
        val withoutMetadata = consumer.receive(ReceiveOptions(readMetadata = false))
        assertNotNull(withoutMetadata)
        assertNull(withoutMetadata.meta)
        assertEquals(id1, withoutMetadata.id)
        assertEquals(data, withoutMetadata.data)
        assertNotSame(data, withoutMetadata.data)
        assertTrue(withoutMetadata.createdAt < withoutMetadata.processingAt, "message=$withoutMetadata")

        // Read second message with metadata and check it
        val withMetadata = consumer.receive(ReceiveOptions(readMetadata = true))
        assertNotNull(withMetadata)
        assertNotNull(withMetadata.meta) {
            assertEquals(2, it.field)
        }
        assertEquals(id2, withMetadata.id)
        assertEquals(data, withMetadata.data)
        assertNotSame(data, withMetadata.data)
        assertTrue(withMetadata.createdAt < withMetadata.processingAt, "message=$withMetadata")
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testReceive_Order(readMetadata: Boolean) {
        val items = 100
        val data = (1..items).map { index ->
            SendMessage("bugaga_$index", TestMeta(index))
        }

        val producer = DatabaseProducer(dataSource, queue)
        producer.send(data)

        val consumer = DatabaseConsumer(dataSource, queue)
        // messages were sent with natural ordering, TestMeta.field is 1,2,3,4...
        // Try to read them in reverse order
        val messages = consumer.receive(
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
            assertTrue(message.createdAt < message.processingAt, "message=$message")
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

        val producer = DatabaseProducer(dataSource, queue)
        producer.send(data)

        val consumer = DatabaseConsumer(dataSource, queue)
        // messages have TestMeta.field from 1 till 100
        // Try to read only messages from 10 till 15
        val start = 10
        val end = 15
        val messages = consumer.receive(
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
            assertTrue(message.createdAt < message.processingAt, "message=$message")
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
