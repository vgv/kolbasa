package kolbasa.consumer.datasource

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReadError
import kolbasa.producer.Id
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendResult.Companion.onlySuccessful
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.DatabaseQueueDataType
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

class DatabaseConsumerReadErrorTest : AbstractPostgreSQLTest() {

    // "bad" can't be deserialized, everything else can
    private val queue = Queue(
        name = "read_error",
        databaseDataType = DatabaseQueueDataType.Text(
            serializer = { data: String -> data },
            deserializer = { data: String -> if (data == "bad") throw IllegalArgumentException("Can't read '$data'") else data }
        ),
        options = QueueOptions(defaultAttempts = 5)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testReadError_Default() {
        val badId = send("bad")
        send("good")

        // By default, the bad message is logged and skipped, the good one is received
        assertEquals(listOf("good"), DatabaseConsumer(dataSource).receive(queue, limit = 10).map { it.data })

        // The bad message spent its attempt, as any received message
        assertEquals(4, remainingAttempts(badId))
    }

    @Test
    fun testReadError_Handler() {
        val badId = send("bad")
        send("good")

        val errors = mutableListOf<ReadError>()
        val consumer = DatabaseConsumer(dataSource, ConsumerOptions(readErrorHandler = { errors += it }))

        // The bad message is skipped, the good one is received
        assertEquals(listOf("good"), consumer.receive(queue, limit = 10).map { it.data })

        // The handler got the bad one
        val error = errors.single()
        assertEquals(queue, error.queue)
        assertEquals(badId, error.messageId)
        assertEquals("", error.nodeId.id)
        assertInstanceOf(IllegalArgumentException::class.java, error.cause)
        assertEquals(4, error.remainingAttempts)
        assertFalse(error.isLastAttempt())

        // Created, then taken for processing, then hidden for the visibility timeout
        assertTrue(error.createdAt <= error.processingAt)
        assertTrue(error.processingAt < error.scheduledAt)

        // The bad message spent its attempt, as any received message, so it reaches the DLQ in the end
        assertEquals(4, remainingAttempts(badId))
    }

    @Test
    fun testReadError_LastAttempt() {
        // Only one attempt, so the first receive() is the last one
        send("bad", MessageOptions(attempts = 1))

        val errors = mutableListOf<ReadError>()
        val consumer = DatabaseConsumer(dataSource, ConsumerOptions(readErrorHandler = { errors += it }))
        assertEquals(emptyList<Message<String>>(), consumer.receive(queue, limit = 10))

        val error = errors.single()
        assertEquals(0, error.remainingAttempts)
        assertTrue(error.isLastAttempt())
    }

    @Test
    fun testReadError_HandlerThrows() {
        val badId = send("bad")
        send("good")

        // A handler that throws fails the whole receive(), and nothing is received
        val consumer = DatabaseConsumer(dataSource, ConsumerOptions(readErrorHandler = { throw it.cause }))
        assertThrows<IllegalArgumentException> { consumer.receive(queue, limit = 10) }

        // The transaction was rolled back, so the message didn't spend its attempt
        assertEquals(5, remainingAttempts(badId))
    }

    private fun send(data: String, options: MessageOptions = MessageOptions.DEFAULT): Id =
        DatabaseProducer(dataSource).send(queue, SendMessage(data, options)).onlySuccessful().single().id

    private fun remainingAttempts(id: Id): Int =
        dataSource.readInt("select remaining_attempts from ${queue.dbTableName} where id = ${id.localId}")
}
