package kolbasa.consumer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.utils.JdbcHelpers.readInt
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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull

private val ALL_LIVE_FIELD = MetaField.int("all_live_unique_field", FieldOption.ALL_LIVE_UNIQUE)
private val UNTOUCHED_FIELD = MetaField.int("untouched_unique_field", FieldOption.UNTOUCHED_UNIQUE)

class DatabaseConsumerDeduplicationTest : AbstractPostgresqlTest() {

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = Metadata.of(ALL_LIVE_FIELD, UNTOUCHED_FIELD)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testDeduplication_All_Live_Unique() {
        val data = "bugaga"
        val messageToSend = SendMessage(data = data, meta = MetaValues.of(ALL_LIVE_FIELD.value(1)), messageOptions = MessageOptions(attempts = 1))

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
        val rawValuesInDatabase = dataSource.readInt("select count(*) from ${queue.dbTableName} where ${ALL_LIVE_FIELD.dbColumnName}=1")
        assertEquals(2, rawValuesInDatabase)
    }

    @Test
    fun testDeduplication_Untouched_Unique() {
        val data = "bugaga"
        val messageToSend = SendMessage(data = data, meta = MetaValues.of(UNTOUCHED_FIELD.value(1)), messageOptions = MessageOptions(attempts = 10))

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

        // ... but, if we read this message, it will move message from AVAILABLE state to IN_FLIGHT
        // technically, it will set processingAt to non-null or, in other words, message will be "touched"
        // in this case, we have to be able to send the message with the same meta field again, even if it has unique constraint
        // Read message and NOT DELETE IT
        run {
            val message = consumer.receive(queue)

            assertNotNull(message)
            assertEquals(id, message.id)
            assertEquals(data, message.data)
            assertNotSame(data, message.data)
            assertTrue(message.createdAt < message.processingAt, "message=$message")
            assertEquals(9, message.remainingAttempts)
        }

        // Send message again with success
        producer.send(queue, messageToSend)

        // Very dirty, raw check directly in the database that we really have two messages
        // with the same meta field value

        val rawValuesInDatabase = dataSource.readInt("select count(*) from ${queue.dbTableName} where ${UNTOUCHED_FIELD.dbColumnName}=1")
        assertEquals(2, rawValuesInDatabase)
    }
}
