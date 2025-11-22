package kolbasa.consumer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendResult.Companion.onlyDuplicated
import kolbasa.producer.SendResult.Companion.onlyFailed
import kolbasa.producer.SendResult.Companion.onlySuccessful
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertTrue

class DatabaseConsumerDeduplicationTest : AbstractPostgresqlTest() {

    private val FIELD = MetaField.int("field", FieldOption.STRICT_UNIQUE)

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIELD)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }

    @Test
    fun testDeduplication_ZeroRemainingAttempts() {
        val data = "bugaga"
        val messageToSend = SendMessage(data = data, meta = MetaValues.of(FIELD.value(1)), messageOptions = MessageOptions(attempts = 1))

        val producer = DatabaseProducer(dataSource)
        val consumer = DatabaseConsumer(dataSource)

        // First send – success
        val id = producer.send(queue, messageToSend).let { (failedMessages, result) ->
            assertEquals(0, failedMessages)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        // Second produce with the same meta field value should fail...
        producer.send(queue, messageToSend).let { (failedMessages, result) ->
            assertEquals(1, failedMessages)
            assertEquals(0, result.onlySuccessful().size)
            assertEquals(0, result.onlyDuplicated().size)
            assertEquals(1, result.onlyFailed().size)
        }

        // ... but, if we read this message, it will set remainingAttempts to zero
        // in this case, we have to be able to send the message with the same meta field again, even if it has unique constraint
        // Read message and NOT DELETE IT
        run {
            val message = consumer.receive(queue)

            assertNotNull(message)
            assertEquals(id, message.id)
            assertEquals(data, message.data)
            assertNotSame(data, message.data)
            assertTrue(message.createdAt < message.processingAt, "message=$message")
            assertEquals(0, message.remainingAttempts)
        }

        // Send message again with success
        producer.send(queue, messageToSend)

        // Very dirty, raw check directly in the database that we really have two messages
        // with the same meta field value
        val rawValuesInDatabase = dataSource.readInt("select count(*) from ${queue.dbTableName} where meta_field=1")
        assertEquals(2, rawValuesInDatabase)
    }
}
