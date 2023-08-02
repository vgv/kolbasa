package kolbasa.consumer

import kolbasa.AbstractPostgresqlTest
import kolbasa.producer.DatabaseProducer
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.Unique
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
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
    fun testReceive_TestConcurrent() {
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

        // Check second message
        assertNotNull(message2)
        assertEquals(id2, message2.id)
        assertEquals(data2, message2.data)
        assertNotSame(data2, message2.data)
        assertTrue(message2.createdAt < message2.processingAt)
    }


    @Test
    fun testReceive_Delay() {
        val data = "bugaga"
        val delay = Duration.of(3000 + Random.nextLong(0, 2000), ChronoUnit.MILLIS)

        val producer = DatabaseProducer(dataSource, queue)
        val id = producer.send(SendMessage(data = data, sendOptions = SendOptions(delay = delay)))
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
        assertTrue((receiveTimestamp - sendTimestamp) > delay.toNanos(), "Receive: $receiveTimestamp, send: $sendTimestamp, delay=$delay")

        // Check message
        assertNotNull(message)
        assertEquals(id, message.id)
        assertEquals(data, message.data)
        assertNotSame(data, message.data)
        assertTrue(message.createdAt < message.processingAt, "message=$message")
    }

}

internal data class TestMeta(@Unique val field: Int)
