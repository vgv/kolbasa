package kolbasa.consumer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.*
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DatabaseConsumerArchiveTest : AbstractPostgresqlTest() {

    private val USER_ID = MetaField.int("user_id")

    private val queue = Queue(
        name = "archive_test",
        databaseDataType = PredefinedDataTypes.String,
        metadata = Metadata.of(USER_ID),
        options = QueueOptions(archiveQueueOptions = ArchiveQueueOptions.DEFAULT)
    )

    private val archiveQueue = requireNotNull(queue.archiveQueue)

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testDeleteMovesToArchive() {
        val producer = DatabaseProducer(dataSource)
        producer.send(queue, "message_1")

        val consumer = DatabaseConsumer(dataSource)
        val message = requireNotNull(consumer.receive(queue))

        // Delete — should move to archive
        val deleted = consumer.delete(queue, message)
        assertEquals(1, deleted)

        // Main queue empty
        assertEquals(0, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
        // Archive has the message
        assertEquals(1, dataSource.readInt("select count(*) from ${archiveQueue.dbTableName}"))
    }

    @Test
    fun testDeletePreservesOriginalValues() {
        val producer = DatabaseProducer(dataSource)
        val meta = MetaValues.of(USER_ID.value(42))
        producer.send(queue, SendMessage("important_data", meta))

        val consumer = DatabaseConsumer(dataSource)
        val message = requireNotNull(consumer.receive(queue))

        // Delete (move to archive)
        consumer.delete(queue, message)

        // Read from archive
        val archiveConsumer = DatabaseConsumer(dataSource)
        val archivedMsg =
            requireNotNull(archiveConsumer.receive(archiveQueue, receiveOptions = ReceiveOptions(readMetadata = true)))
        assertEquals("important_data", archivedMsg.data)

        // Check original values is preserved in meta
        assertEquals(message.id.localId, archivedMsg.meta.get(Metadata.ARCHIVE_ORIGINAL_ID))
        // TODO: fix timezone issues to enable these checks
        //assertEquals(message.createdAt, archivedMsg.meta.get(Metadata.ARCHIVE_ORIGINAL_CREATED_AT))
        //assertEquals(message.processingAt, archivedMsg.meta.get(Metadata.ARCHIVE_ORIGINAL_PROCESSING_AT))
        assertEquals(message.remainingAttempts, archivedMsg.meta.get(Metadata.ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS))

        // Check user-defined meta is preserved
        assertEquals(42, archivedMsg.meta.get(USER_ID))
    }

    @Test
    fun testDeleteMultipleMessagesMovesToArchive() {
        val producer = DatabaseProducer(dataSource)
        val messages = (1..5).map { "message_$it" }
        messages.forEach { producer.send(queue, it) }

        val consumer = DatabaseConsumer(dataSource)
        val received = consumer.receive(queue, limit = 5)
        assertEquals(5, received.size)

        // Delete all
        val deleted = consumer.delete(queue, received)
        assertEquals(5, deleted)

        // Main queue empty
        assertEquals(0, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
        // Archive has all messages
        assertEquals(5, dataSource.readInt("select count(*) from ${archiveQueue.dbTableName}"))
    }

    @Test
    fun testWithoutArchive_DeleteIsPlainDelete() {
        val plainQueue = Queue.of("plain_test", PredefinedDataTypes.String)
        SchemaHelpers.createOrUpdateQueues(dataSource, plainQueue)

        val producer = DatabaseProducer(dataSource)
        producer.send(plainQueue, "message_1")

        val consumer = DatabaseConsumer(dataSource)
        val message = requireNotNull(consumer.receive(plainQueue))

        // Delete — plain delete, no archive
        consumer.delete(plainQueue, message)

        assertEquals(0, dataSource.readInt("select count(*) from ${plainQueue.dbTableName}"))
    }
}
