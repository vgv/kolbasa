package kolbasa.producer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.producer.DeduplicationMode
import kolbasa.producer.Id
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Searchable
import kolbasa.queue.Unique
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertSame

class DatabaseProducerDeduplicationTest : AbstractPostgresqlTest() {

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
    fun testDeduplication_ERROR_SingleMessage() {
        val messageToSend = SendMessage("bugaga", TestMeta(1))
        val producer = DatabaseProducer(
            dataSource,
            queue,
            ProducerOptions(deduplicationMode = DeduplicationMode.ERROR)
        )

        // First send – success
        producer.send(messageToSend)

        // Second send with the same meta field value should fail
        assertFails {
            producer.send(messageToSend)
        }

        // raw database check
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testDeduplication_ERROR_MessagesList() {
        val messageToSend = SendMessage("bugaga", TestMeta(1))
        val producer = DatabaseProducer(
            dataSource,
            queue,
            ProducerOptions(deduplicationMode = DeduplicationMode.ERROR, batchSize = 1000)
        )

        // First send – success
        producer.send(messageToSend)

        // Second send several messages with one poison message should fail completely
        val messagesWithPoisonMessage = (100 downTo 1).map {
            SendMessage("bugaga_$it", TestMeta(it))
        }
        // this call should not insert anything, because there is the poison message with TestMeta.field == 1
        val sendResult = producer.send(messagesWithPoisonMessage)
        assertEquals(0, sendResult.onlySuccessful().size) // zero really inserted
        assertEquals(0, sendResult.onlyDuplicated().size) // zero duplicates
        assertEquals(1, sendResult.onlyFailed().size) // One error containing all 100 messages, because nothing was inserted
        assertEquals(100, sendResult.failedMessages) // 100 messages failed, because the whole list failed


        // raw database check
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testDeduplication_IGNORE_DUPLICATES_SingleMessage() {
        val messageToSend = SendMessage("bugaga", TestMeta(1))
        val producer = DatabaseProducer(
            dataSource,
            queue,
            ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATES)
        )

        // First send – success
        producer.send(messageToSend)

        // Second send with the same meta field value should return Const.RESERVED_DUPLICATE_ID and not insert anything
        assertSame(Id.DEFAULT_DUPLICATE_ID, producer.send(messageToSend))

        // raw database check
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testDeduplication_IGNORE_DUPLICATES_MessagesList() {
        val messageToSend = SendMessage("bugaga", TestMeta(1))
        val producer = DatabaseProducer(
            dataSource,
            queue,
            ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATES, batchSize = 1000)
        )

        // First send – success
        producer.send(messageToSend)

        // Second send several messages with one poison message should insert 99 messages
        val messagesWithPoisonMessage = (100 downTo 1).map {
            SendMessage("bugaga_$it", TestMeta(it))
        }
        val sendResult = producer.send(messagesWithPoisonMessage)

        assertEquals(99, sendResult.onlySuccessful().size) // 99 really inserted
        assertEquals(1, sendResult.onlyDuplicated().size) // 1 duplicate
        assertEquals(0, sendResult.onlyFailed().size) // zero errors

        // raw database check
        assertEquals(100, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }


}
