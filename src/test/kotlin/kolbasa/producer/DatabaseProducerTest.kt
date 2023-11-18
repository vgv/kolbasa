package kolbasa.producer

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Unique
import kolbasa.schema.Const
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertIs
import kotlin.test.assertNotNull

class DatabaseProducerTest : AbstractPostgresqlTest() {

    private val queue = Queue(
        "local",
        PredefinedDataTypes.String,
        metadata = TestMeta::class.java
    )

    private val first = listOf(
        SendMessage("bugaga", TestMeta(1)),
        SendMessage("bugaga", TestMeta(2)),
        SendMessage("bugaga", TestMeta(3)),
        SendMessage("bugaga", TestMeta(4)),
        SendMessage("bugaga", TestMeta(5))
    )
    private val second = listOf(
        SendMessage("bugaga", TestMeta(6)),
        SendMessage("bugaga", TestMeta(7)),
        SendMessage("bugaga", TestMeta(2)), // POISON MESSAGE WITH NON UNIQUE META FIELD
        SendMessage("bugaga", TestMeta(9)),
        SendMessage("bugaga", TestMeta(10))
    )
    private val third = listOf(
        SendMessage("bugaga", TestMeta(11)),
        SendMessage("bugaga", TestMeta(12)),
        SendMessage("bugaga", TestMeta(13)),
        SendMessage("bugaga", TestMeta(14)),
        SendMessage("bugaga", TestMeta(15))
    )
    private val items = first + second + third

    @BeforeEach
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }

    @Test
    fun testSendSimpleData() {
        val producer = DatabaseProducer(dataSource, queue)

        val id1 = producer.send("bugaga")
        val id2 = producer.send("bugaga")
        assertEquals(1, id1)
        assertEquals(2, id2)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendSimpleData_WithCustomProducerName() {
        val firstProducerName = "first_producer"
        val secondProducerName = "second_producer"
        val firstProducer = DatabaseProducer(dataSource, queue, ProducerOptions(producer = firstProducerName))
        val secondProducer = DatabaseProducer(dataSource, queue, ProducerOptions(producer = secondProducerName))

        val id1 = firstProducer.send("bugaga")
        val id2 = secondProducer.send("bugaga")
        assertEquals(1, id1)
        assertEquals(2, id2)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
        // check first producer
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName} where ${Const.PRODUCER_COLUMN_NAME}='$firstProducerName'"))
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName} where ${Const.PRODUCER_COLUMN_NAME}='$secondProducerName'"))
    }


    @Test
    fun testSendSimpleDataAsSendMessage() {
        val producer = DatabaseProducer(dataSource, queue)

        val id1 = producer.send(SendMessage("bugaga", TestMeta(1)))
        val id2 = producer.send(SendMessage("bugaga", TestMeta(2)))
        assertEquals(1, id1)
        assertEquals(2, id2)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendSimpleDataAsSendMessage_IfError() {
        val producer = DatabaseProducer(dataSource, queue)

        producer.send(SendMessage("bugaga", TestMeta(1)))

        assertFails {
            producer.send(SendMessage("bugaga", TestMeta(1)))
        }

        // check database
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendProhibited() {
        val producer = DatabaseProducer(
            dataSource,
            queue,
            ProducerOptions(batchSize = 5, partialInsert = PartialInsert.PROHIBITED)
        )

        val result = producer.send(items)

        assertEquals(items.size, result.failedMessages) // all messages failed
        assertEquals(items, result.gatherFailedMessages())
        assertEquals(1, result.messages.size) // all failed messages in one error message

        val messages = result.messages[0]
        assertIs<MessageResult.Error<String, TestMeta>>(messages)
        assertNotNull(messages.exception)
        assertEquals(items, messages.messages)

        // check database
        assertEquals(0, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendUntilFirstFailure() {
        val producer = DatabaseProducer(
            dataSource,
            queue,
            ProducerOptions(batchSize = 5, partialInsert = PartialInsert.UNTIL_FIRST_FAILURE)
        )

        val result = producer.send(items)

        assertEquals(10, result.failedMessages) // 10 of 15 messages are failed
        assertEquals(second + third, result.gatherFailedMessages())
        assertEquals(6, result.messages.size) // 5 good + 1 bad

        // first 5 items are good
        first.forEachIndexed { index, sendMessage ->
            assertIs<MessageResult.Success<String, TestMeta>>(result.messages[index]).let {
                assertEquals(index + 1L, it.id)
                assertEquals(sendMessage, it.message)
            }
        }

        // Last one is bad
        assertIs<MessageResult.Error<String, TestMeta>>(result.messages[5]).let {
            assertEquals(second + third, it.messages)
        }

        // check database
        assertEquals(5, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendAsManyAsPossible() {
        val producer = DatabaseProducer(
            dataSource,
            queue,
            ProducerOptions(batchSize = 5, partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)
        )

        val result = producer.send(items)

        assertEquals(5, result.failedMessages) // 5 of 15 messages are failed
        assertEquals(second, result.gatherFailedMessages())
        assertEquals(11, result.messages.size) // 5 good + 1 bad + 5 good

        // first 5 items are good
        first.forEachIndexed { index, sendMessage ->
            assertIs<MessageResult.Success<String, TestMeta>>(result.messages[index]).let {
                assertEquals(index + 1L, it.id)
                assertEquals(sendMessage, it.message)
            }
        }

        // Next one is bad
        assertIs<MessageResult.Error<String, TestMeta>>(result.messages[5]).let {
            assertEquals(second, it.messages)
        }

        // Next 5 are good again
        third.forEachIndexed { index, sendMessage ->
            assertIs<MessageResult.Success<String, TestMeta>>(result.messages[index + 6]).let {
                assertEquals(index + 9L, it.id)
                assertEquals(sendMessage, it.message)
            }
        }

        // check database
        assertEquals(10, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

}

internal data class TestMeta(@Unique val field: Int)
