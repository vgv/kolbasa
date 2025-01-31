package kolbasa.producer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.producer.MessageResult
import kolbasa.producer.PartialInsert
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendResult.Companion.onlySuccessful
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Unique
import kolbasa.schema.Const
import kolbasa.schema.IdRange
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
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
        val producer = DatabaseProducer(dataSource)

        val result1 = producer.send(queue, "bugaga")
        assertEquals(0, result1.failedMessages)
        assertEquals(1, result1.onlySuccessful().size)
        val id1 = result1.onlySuccessful().first().id

        val result2 = producer.send(queue, "bugaga")
        assertEquals(0, result2.failedMessages)
        assertEquals(1, result2.onlySuccessful().size)
        val id2 = result2.onlySuccessful().first().id

        assertEquals(IdRange.LOCAL_RANGE.start, id1.localId)
        assertEquals(IdRange.LOCAL_RANGE.start + 1, id2.localId)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendSimpleData_WithCustomProducerName() {
        val firstProducerName = "first_producer"
        val secondProducerName = "second_producer"
        val firstProducer = DatabaseProducer(dataSource, ProducerOptions(producer = firstProducerName))
        val secondProducer = DatabaseProducer(dataSource, ProducerOptions(producer = secondProducerName))

        val result1 = firstProducer.send(queue, "bugaga")
        assertEquals(0, result1.failedMessages)
        assertEquals(1, result1.onlySuccessful().size)
        val id1 = result1.onlySuccessful().first().id

        val result2 = secondProducer.send(queue, "bugaga")
        assertEquals(0, result2.failedMessages)
        assertEquals(1, result2.onlySuccessful().size)
        val id2 = result2.onlySuccessful().first().id

        assertEquals(IdRange.LOCAL_RANGE.start, id1.localId)
        assertEquals(IdRange.LOCAL_RANGE.start + 1, id2.localId)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
        // check first producer
        assertEquals(
            1,
            dataSource.readInt("select count(*) from ${queue.dbTableName} where ${Const.PRODUCER_COLUMN_NAME}='$firstProducerName'")
        )
        assertEquals(
            1,
            dataSource.readInt("select count(*) from ${queue.dbTableName} where ${Const.PRODUCER_COLUMN_NAME}='$secondProducerName'")
        )
    }


    @Test
    fun testSendSimpleDataAsSendMessage() {
        val producer = DatabaseProducer(dataSource)

        val id1 = producer.send(queue, SendMessage("bugaga", TestMeta(1))).let { (failedMessages, result) ->
            assertEquals(0, failedMessages)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        val id2 = producer.send(queue, SendMessage("bugaga", TestMeta(2))).let { (failedMessages, result) ->
            assertEquals(0, failedMessages)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        assertEquals(IdRange.LOCAL_RANGE.start, id1.localId)
        assertEquals(IdRange.LOCAL_RANGE.start + 1, id2.localId)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendProhibited() {
        val producer = DatabaseProducer(
            dataSource,
            ProducerOptions(batchSize = 5, partialInsert = PartialInsert.PROHIBITED)
        )

        val result = producer.send(queue, items)

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
            ProducerOptions(batchSize = 5, partialInsert = PartialInsert.UNTIL_FIRST_FAILURE)
        )

        val result = producer.send(queue, items)

        assertEquals(10, result.failedMessages) // 10 of 15 messages are failed
        assertEquals(second + third, result.gatherFailedMessages())
        assertEquals(6, result.messages.size) // 5 good + 1 bad

        // first 5 items are good
        first.forEachIndexed { index, sendMessage ->
            assertIs<MessageResult.Success<String, TestMeta>>(result.messages[index]).let {
                assertEquals(index + IdRange.LOCAL_RANGE.start, it.id.localId)
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
            ProducerOptions(batchSize = 5, partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)
        )

        val result = producer.send(queue, items)

        assertEquals(5, result.failedMessages) // 5 of 15 messages are failed
        assertEquals(second, result.gatherFailedMessages())
        assertEquals(11, result.messages.size) // 5 good + 1 bad + 5 good

        // first 5 items are good
        first.forEachIndexed { index, sendMessage ->
            assertIs<MessageResult.Success<String, TestMeta>>(result.messages[index]).let {
                assertEquals(index +IdRange.LOCAL_RANGE.start, it.id.localId)
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
                assertEquals(index + IdRange.LOCAL_RANGE.start + 8, it.id.localId)
                assertEquals(sendMessage, it.message)
            }
        }

        // check database
        assertEquals(10, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

}

internal data class TestMeta(@Unique val field: Int)
