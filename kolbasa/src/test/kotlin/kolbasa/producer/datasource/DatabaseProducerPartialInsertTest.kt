package kolbasa.producer.datasource

import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.producer.MessageResult
import kolbasa.producer.PartialInsert
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.IdRange
import kolbasa.schema.SchemaHelpers
import kolbasa.utils.JdbcHelpers.inTransaction
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertNotNull

class DatabaseProducerPartialInsertTest : AbstractPostgreSQLTest() {

    private val FIELD = MetaField.ofInt("field", FieldOption.ALL_LIVE_UNIQUE)

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

    // The serializer fails on "boom", outside PostgreSQL
    private val failingQueue = Queue(
        name = "failing_serializer",
        databaseDataType = DatabaseQueueDataType.Text(
            serializer = { data: String -> if (data == "boom") error("Can't write '$data'") else data },
            deserializer = { data: String -> data }
        )
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue, failingQueue)
    }

    /**
     * PROHIBITED: one bad chunk fails the whole send, and nothing is saved.
     *
     * 15 messages go in 3 chunks of 5. The second chunk repeats the unique field value 2 from the first chunk. A
     * duplicate is an error by default, so PostgreSQL rejects the second chunk. By then the first chunk is already
     * inserted, and the savepoint takes it back. The third chunk is never sent.
     *
     * Expected: all 15 messages failed, in one error with its cause, and the table is empty.
     */
    @Test
    fun testSend_PartialInsert_Prohibited() {
        val producer = DatabaseProducer(
            dataSource,
            ProducerOptions(chunkSize = 5, partialInsert = PartialInsert.PROHIBITED)
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

    /**
     * PROHIBITED, an error outside PostgreSQL: nothing is saved either.
     *
     * 3 messages, one per chunk. The first chunk is inserted. Then the serializer of the queue throws on "boom", before
     * the second chunk reaches PostgreSQL. PostgreSQL sees no error, so the transaction stays alive, and
     * [DatabaseProducer] commits it. Only the savepoint takes the first chunk back.
     *
     * Expected: all 3 messages failed, and the table is empty.
     */
    @Test
    fun testSend_PartialInsert_Prohibited_SerializerFails() {
        assertSerializerFailureSavesNothing(chunkSize = 1)
    }

    /**
     * The same as [testSend_PartialInsert_Prohibited_SerializerFails], but all 3 messages go in one chunk.
     *
     * The serializer throws while the INSERT is being prepared, so nothing reaches PostgreSQL at all.
     *
     * Expected: all 3 messages failed, and the table is empty.
     */
    @Test
    fun testSend_PartialInsert_Prohibited_SerializerFails_OneChunk() {
        assertSerializerFailureSavesNothing(chunkSize = 5)
    }

    private fun assertSerializerFailureSavesNothing(chunkSize: Int) {
        val producer = DatabaseProducer(
            dataSource,
            ProducerOptions(chunkSize = chunkSize, partialInsert = PartialInsert.PROHIBITED)
        )
        val messages = listOf(SendMessage("first"), SendMessage("boom"), SendMessage("third"))

        val result = producer.send(failingQueue, messages)

        assertEquals(messages.size, result.failedMessages) // all messages failed
        assertEquals(messages, result.gatherFailedMessages())

        // check database: the first message isn't saved either
        assertEquals(0, dataSource.readInt("select count(*) from ${failingQueue.dbTableName}"))
    }

    /**
     * PROHIBITED in your own transaction, an error outside PostgreSQL: the transaction stays alive and clean.
     *
     * [ConnectionAwareDatabaseProducer], one message per chunk. "first" is inserted, then the serializer throws on
     * "boom", and the savepoint takes "first" back. After that, a good message is sent in the same transaction, and the
     * transaction is committed.
     *
     * Expected: the send failed, and the table holds only the good message. So the transaction was alive, and "first"
     * wasn't in it.
     */
    @Test
    fun testSend_PartialInsert_Prohibited_OwnTransaction_SerializerFails() {
        val failing = listOf(SendMessage("first"), SendMessage("boom"))
        assertOwnTransactionStaysClean(failingQueue, failing, SendMessage("good"), chunkSize = 1)
    }

    /**
     * PROHIBITED in your own transaction, an error in PostgreSQL: the transaction stays alive too.
     *
     * [ConnectionAwareDatabaseProducer], 6 messages in one chunk. The last one repeats the unique field value 1, so
     * PostgreSQL rejects the INSERT. Without the savepoint, PostgreSQL would abort the whole transaction: every later
     * statement would fail, and the commit would turn into a rollback. With it, a good message is sent in the same
     * transaction, and the transaction is committed.
     *
     * Expected: the send failed, and the table holds only the good message.
     */
    @Test
    fun testSend_PartialInsert_Prohibited_OwnTransaction_SqlFails() {
        val failing = first + SendMessage("bugaga", MetaValues.of(FIELD.value(1)))
        val good = SendMessage("good", MetaValues.of(FIELD.value(100)))
        assertOwnTransactionStaysClean(queue, failing, good, chunkSize = 500)
    }

    private fun <Data> assertOwnTransactionStaysClean(
        queue: Queue<Data>,
        failing: List<SendMessage<Data>>,
        good: SendMessage<Data>,
        chunkSize: Int
    ) {
        val producer = ConnectionAwareDatabaseProducer(
            ProducerOptions(chunkSize = chunkSize, partialInsert = PartialInsert.PROHIBITED)
        )

        dataSource.inTransaction { connection ->
            val result = producer.send(connection, queue, failing)
            assertEquals(failing.size, result.failedMessages) // all messages failed

            // The transaction is alive and holds none of the failed messages, so it can go on and commit
            assertEquals(0, producer.send(connection, queue, good).failedMessages)
        }

        // check database: only the message sent after the failure
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    /**
     * UNTIL_FIRST_FAILURE: the chunks before the bad one are saved, the bad one and all after it fail.
     *
     * The same 15 messages as in [testSend_PartialInsert_Prohibited], in 3 chunks of 5. The first chunk is saved.
     * PostgreSQL rejects the second one, and its savepoint takes it back. The third chunk is fine, but it isn't sent.
     *
     * Expected: the first 5 messages are sent, with ids in send order. The other 10 failed, in one error. The table has
     * 5 rows.
     */
    @Test
    fun testSend_PartialInsert_UntilFirstFailure() {
        val producer = DatabaseProducer(
            dataSource,
            ProducerOptions(chunkSize = 5, partialInsert = PartialInsert.UNTIL_FIRST_FAILURE)
        )

        val result = producer.send(queue, items)

        assertEquals(10, result.failedMessages) // 10 of 15 messages are failed
        assertEquals(second + third, result.gatherFailedMessages())
        assertEquals(6, result.messages.size) // 5 good + 1 bad

        // first 5 items are good
        first.forEachIndexed { index, sendMessage ->
            assertInstanceOf<MessageResult.Success<String>>(result.messages[index]).let {
                assertEquals(index + IdRange.MIN_RANGE.min, it.id.localId)
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

    /**
     * INSERT_AS_MANY_AS_POSSIBLE: only the bad chunk fails, the chunks before and after it are saved.
     *
     * The same 15 messages as in [testSend_PartialInsert_Prohibited], in 3 chunks of 5. The first chunk is saved.
     * PostgreSQL rejects the second one, and its savepoint takes it back. The third chunk is saved too.
     *
     * Expected: 10 messages are sent, the 5 of the second chunk failed, in one error. The table has 10 rows. The ids
     * of the third chunk start at 8, not at 5: a rollback doesn't give sequence values back, and the rejected chunk
     * used up 3 of them before PostgreSQL hit the duplicate.
     */
    @Test
    fun testSend_PartialInsert_AsManyAsPossible() {
        val producer = DatabaseProducer(
            dataSource,
            ProducerOptions(chunkSize = 5, partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)
        )

        val result = producer.send(queue, items)

        assertEquals(5, result.failedMessages) // 5 of 15 messages are failed
        assertEquals(second, result.gatherFailedMessages())
        assertEquals(11, result.messages.size) // 5 good + 1 bad + 5 good

        // first 5 items are good
        first.forEachIndexed { index, sendMessage ->
            assertInstanceOf<MessageResult.Success<String>>(result.messages[index]).let {
                assertEquals(index + IdRange.MIN_RANGE.min, it.id.localId)
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
                assertEquals(index + IdRange.MIN_RANGE.min + 8, it.id.localId)
                assertEquals(sendMessage, it.message)
            }
        }

        // check database
        assertEquals(10, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

}
