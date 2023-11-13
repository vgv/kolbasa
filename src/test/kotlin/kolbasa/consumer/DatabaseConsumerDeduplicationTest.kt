package kolbasa.consumer

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.producer.DatabaseProducer
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Searchable
import kolbasa.queue.Unique
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.*

class DatabaseConsumerDeduplicationTest : AbstractPostgresqlTest() {

    internal data class TestMeta(@Searchable @Unique val field: Int)

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
    fun testDeduplication_FailOnUnique() {
        // Just simple test to check that @Unique works
        val messageToSend = SendMessage("bugaga", TestMeta(1))
        val producer = DatabaseProducer(dataSource, queue)

        // First send – success
        producer.send(messageToSend)

        // Second send with the same meta field value should fail
        assertFails {
            producer.send(messageToSend)
        }
    }


    @Test
    fun testDeduplication_ZeroRemainingAttempts() {
        val data = "bugaga"
        val messageToSend = SendMessage(data, TestMeta(1), SendOptions(attempts = 1))

        val producer = DatabaseProducer(dataSource, queue)
        val consumer = DatabaseConsumer(dataSource, queue)

        // First send – success
        val id = producer.send(messageToSend)

        // Second produce with the same meta field value should fail...
        assertFails { producer.send(messageToSend) }

        // ... but, if we read this message, it will set remainingAttempts to zero
        // in this case, we have to be able to send the message with the same meta field again, even if it has unique constraint
        // Read message and NOT DELETE IT
        run {
            val message = consumer.receive()

            assertNotNull(message)
            assertEquals(id, message.id)
            assertEquals(data, message.data)
            assertNotSame(data, message.data)
            assertTrue(message.createdAt < message.processingAt, "message=$message")
            assertEquals(0, message.remainingAttempts)
        }

        // Send message again with success
        producer.send(messageToSend)

        // Very dirty, raw check directly in the database that we really have two messages
        // with the same meta field value
        val rawValuesInDatabase = dataSource.readInt("select count(*) from ${queue.dbTableName} where meta_field=1")
        assertEquals(2, rawValuesInDatabase)
    }
}
