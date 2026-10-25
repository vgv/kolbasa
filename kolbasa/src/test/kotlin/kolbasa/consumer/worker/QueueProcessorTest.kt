package kolbasa.consumer.worker

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException

class QueueProcessorTest : AbstractPostgreSQLTest() {

    private val queue = Queue(
        name = "processor",
        databaseDataType = PredefinedDataTypes.String,
        options = QueueOptions(defaultAttempts = 5)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testProcess_Empty() {
        val processor = QueueProcessor(DatabaseConsumer(dataSource), queue) {
            error("No messages, nothing to handle")
        }

        val result = processor.process()
        assertEquals(ProcessResult(succeeded = 0, skipped = 0, failed = 0, requested = 10), result)
        assertEquals(0, result.received)
        assertFalse(result.isBatchFull)
    }

    @Test
    fun testProcess_Success() {
        send("one", "two", "three")

        val handled = mutableListOf<String>()
        val processor = QueueProcessor(DatabaseConsumer(dataSource), queue) { message ->
            handled += message.data
            HandleResult.DONE
        }

        // Every message goes to the handler, and the handled ones are deleted
        val result = processor.process()
        assertEquals(ProcessResult(succeeded = 3, skipped = 0, failed = 0, requested = 10), result)
        assertEquals(3, result.received)

        // Three messages of ten possible: the queue had nothing more to give
        assertFalse(result.isBatchFull)
        assertEquals(setOf("one", "two", "three"), handled.toSet())
        assertEquals(0, messagesInQueue())
    }

    @Test
    fun testProcess_Skip() {
        send("now", "later")

        val errors = mutableListOf<ProcessError>()
        val options = QueueProcessorOptions(errorHandler = { errors += it })
        val processor = QueueProcessor(DatabaseConsumer(dataSource), queue, options) { message ->
            if (message.data == "now") HandleResult.DONE else HandleResult.SKIP
        }

        assertEquals(ProcessResult(succeeded = 1, skipped = 1, failed = 0, requested = 10), processor.process())

        // A skipped message is not an error
        assertEquals(emptyList<ProcessError>(), errors)

        // The handled message is deleted, the skipped one stays in the queue with one attempt spent
        assertEquals(1, messagesInQueue())
        assertEquals(4, dataSource.readInt("select remaining_attempts from ${queue.dbTableName}"))
    }

    @Test
    fun testProcess_HandlerFails() {
        send("good", "bad")

        val errors = mutableListOf<ProcessError>()
        val options = QueueProcessorOptions(errorHandler = { errors += it })
        val processor = QueueProcessor(DatabaseConsumer(dataSource), queue, options) { message ->
            if (message.data == "bad") throw IllegalArgumentException("Can't handle '${message.data}'")
            HandleResult.DONE
        }

        assertEquals(ProcessResult(succeeded = 1, skipped = 0, failed = 1, requested = 10), processor.process())

        // The error handler got the failed message
        val error = errors.single()
        assertEquals(queue, error.queue)
        assertEquals("bad", error.message.data)
        assertInstanceOf(IllegalArgumentException::class.java, error.cause)

        // The good message is deleted, the bad one stays in the queue with one attempt spent
        assertEquals(1, messagesInQueue())
        assertEquals(4, dataSource.readInt("select remaining_attempts from ${queue.dbTableName}"))
    }

    @Test
    fun testProcess_ReceiveLimit() {
        send("one", "two", "three", "four", "five")

        val options = QueueProcessorOptions(receiveLimit = 2)
        val processor = QueueProcessor(DatabaseConsumer(dataSource), queue, options) { HandleResult.DONE }

        // One call takes one batch only, and it is full: the queue has more
        val result = processor.process()
        assertEquals(ProcessResult(succeeded = 2, skipped = 0, failed = 0, requested = 2), result)
        assertTrue(result.isBatchFull)
        assertEquals(3, messagesInQueue())
    }

    @Test
    fun testProcess_ReceiveFails() {
        // No queue table, so receive() fails, and process() passes the exception on
        SchemaHelpers.deleteQueues(dataSource, queue)

        val processor = QueueProcessor(DatabaseConsumer(dataSource), queue) { HandleResult.DONE }
        assertThrows<SQLException> { processor.process() }
    }

    @Test
    fun testOptions_ReceiveLimit() {
        assertThrows<IllegalArgumentException> { QueueProcessorOptions(receiveLimit = 0) }
    }

    private fun send(vararg data: String) {
        DatabaseProducer(dataSource).send(queue, data.map { SendMessage(it) }).throwExceptionIfAny()
    }

    private fun messagesInQueue(): Int = dataSource.readInt("select count(*) from ${queue.dbTableName}")
}
