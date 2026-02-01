package kolbasa.producer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.producer.MessageResult
import kolbasa.producer.PartialInsert
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.IdRange
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertNotNull

class DatabaseProducerPartialInsertTest : AbstractPostgresqlTest() {

    private val FIELD = MetaField.int("field", FieldOption.ALL_LIVE_UNIQUE)

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIELD)
    )

    private val first = listOf(
        SendMessage("bugaga", MetaValues.of(FIELD.value(1))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(2))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(3))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(4))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(5)))
    )
    private val second = listOf(
        SendMessage("bugaga", MetaValues.of(FIELD.value(6))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(7))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(2))), // POISON MESSAGE WITH NON UNIQUE META FIELD
        SendMessage("bugaga", MetaValues.of(FIELD.value(9))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(10)))
    )
    private val third = listOf(
        SendMessage("bugaga", MetaValues.of(FIELD.value(11))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(12))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(13))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(14))),
        SendMessage("bugaga", MetaValues.of(FIELD.value(15)))
    )
    private val items = first + second + third

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testSend_PartialInsert_Prohibited() {
        val producer = DatabaseProducer(
            dataSource,
            ProducerOptions(batchSize = 5, partialInsert = PartialInsert.PROHIBITED)
        )

        val result = producer.send(queue, items)

        assertEquals(items.size, result.failedMessages) // all messages failed
        assertEquals(items, result.gatherFailedMessages())
        assertEquals(1, result.messages.size) // all failed messages in one error message

        val messages = result.messages[0]
        assertInstanceOf<MessageResult.Error<String>>(messages)
        assertNotNull(messages.exception)
        assertEquals(items, messages.messages)

        // check database
        assertEquals(0, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSend_PartialInsert_UntilFirstFailure() {
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
            assertInstanceOf<MessageResult.Success<String>>(result.messages[index]).let {
                assertEquals(index + IdRange.LOCAL_RANGE.min, it.id.localId)
                assertEquals(sendMessage, it.message)
            }
        }

        // Last one is bad
        assertInstanceOf<MessageResult.Error<String>>(result.messages[5]).let {
            assertEquals(second + third, it.messages)
        }

        // check database
        assertEquals(5, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSend_PartialInsert_AsManyAsPossible() {
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
            assertInstanceOf<MessageResult.Success<String>>(result.messages[index]).let {
                assertEquals(index + IdRange.LOCAL_RANGE.min, it.id.localId)
                assertEquals(sendMessage, it.message)
            }
        }

        // Next one is bad
        assertInstanceOf<MessageResult.Error<String>>(result.messages[5]).let {
            assertEquals(second, it.messages)
        }

        // Next 5 are good again
        third.forEachIndexed { index, sendMessage ->
            assertInstanceOf<MessageResult.Success<String>>(result.messages[index + 6]).let {
                assertEquals(index + IdRange.LOCAL_RANGE.min + 8, it.id.localId)
                assertEquals(sendMessage, it.message)
            }
        }

        // check database
        assertEquals(10, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

}
