package kolbasa.queue

import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.Metadata
import kolbasa.schema.Const
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration

class QueueTest {

    private val USER_ID = MetaField.int("user_id")
    private val NAME = MetaField.string("name")

    @Test
    fun testBuilders_WithoutMetadata_WithoutOptions() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            metadata = Metadata.EMPTY
        )

        val expectedBuilder = Queue.builder("1", PredefinedDataTypes.String).build()
        val expectedOf = Queue.of("1", PredefinedDataTypes.String)

        assertEquals(expectedBuilder, actual)
        assertEquals(expectedOf, actual)
    }

    @Test
    fun testBuilders_WithMetadata() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            metadata = Metadata.of(USER_ID, NAME)
        )

        val expectedBuilder = Queue.builder("1", PredefinedDataTypes.String)
            .metadata(Metadata.of(USER_ID, NAME))
            .build()
        val expectedOf = Queue.of("1", PredefinedDataTypes.String, Metadata.of(USER_ID, NAME))

        assertEquals(expectedBuilder, actual)
        assertEquals(expectedOf, actual)
    }

    @Test
    fun testBuilders_WithOptions() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)),
            metadata = Metadata.EMPTY
        )

        val expected = Queue.builder("1", PredefinedDataTypes.String)
            .options(QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)))
            .build()

        assertEquals(expected, actual)
    }

    @Test
    fun testBuilders_WithMetadata_WithOptions() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)),
            metadata = Metadata.of(USER_ID, NAME)
        )

        val expected = Queue.builder("1", PredefinedDataTypes.String)
            .metadata(Metadata.of(USER_ID, NAME))
            .options(QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)))
            .build()

        assertEquals(expected, actual)
    }

    @Test
    fun testDlqCompanionQueueCreation() {
        val queue = Queue(
            name = "orders",
            databaseDataType = PredefinedDataTypes.String,
            metadata = Metadata.of(USER_ID, NAME),
            options = QueueOptions(dlqOptions = DlqOptions.DEFAULT)
        )

        assertNull(queue.archiveQueue)

        val dlq = requireNotNull(queue.deadLetterQueue)
        assertEquals("orders${Const.DLQ_TABLE_NAME_SUFFIX}", dlq.name)
        assertEquals(QueueType.DLQ, dlq.queueType)
        assertNull(dlq.deadLetterQueue)
        assertNull(dlq.archiveQueue)

        // DLQ should have parent fields (with NONE option) + 4 original-value fields
        assertEquals(2 + 4, dlq.metadata.fields.size)

        // Parent fields must have NONE option (indexes stripped)
        val userId = requireNotNull(dlq.metadata.findByName(USER_ID.name))
        assertEquals(FieldOption.NONE, userId.option)
        val name = requireNotNull(dlq.metadata.findByName(NAME.name))
        assertEquals(FieldOption.NONE, name.option)

        // DLQ-specific original-value fields
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_ID.name))
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_CREATED_AT.name))
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_PROCESSING_AT.name))
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_SCHEDULED_AT.name))
    }

    @Test
    fun testArchiveCompanionQueueCreation() {
        val queue = Queue(
            name = "orders",
            databaseDataType = PredefinedDataTypes.String,
            metadata = Metadata.of(USER_ID, NAME),
            options = QueueOptions(archiveQueueOptions = ArchiveQueueOptions.DEFAULT)
        )

        assertNull(queue.deadLetterQueue)

        val archive = requireNotNull(queue.archiveQueue)
        assertEquals("orders${Const.ARCHIVE_TABLE_NAME_SUFFIX}", archive.name)
        assertEquals(QueueType.ARCHIVE, archive.queueType)
        assertNull(archive.deadLetterQueue)
        assertNull(archive.archiveQueue)

        // Archive should have parent fields (with NONE option) + 4 original-value fields
        assertEquals(2 + 4, archive.metadata.fields.size)

        // Parent fields must have NONE option (indexes stripped)
        val userId = requireNotNull(archive.metadata.findByName(USER_ID.name))
        assertEquals(FieldOption.NONE, userId.option)
        val name = requireNotNull(archive.metadata.findByName(NAME.name))
        assertEquals(FieldOption.NONE, name.option)

        // Archive-specific original-value fields
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_ID.name))
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_CREATED_AT.name))
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS.name))
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_PROCESSING_AT.name))
    }

    @Test
    fun testBothCompanionQueues() {
        val queue = Queue(
            name = "orders",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(
                dlqOptions = DlqOptions.DEFAULT,
                archiveQueueOptions = ArchiveQueueOptions.DEFAULT
            )
        )

        assertNotNull(queue.deadLetterQueue)
        assertNotNull(queue.archiveQueue)
    }

    @Test
    fun testNoCompanionQueuesWithoutOptions() {
        val queue = Queue(
            name = "orders",
            databaseDataType = PredefinedDataTypes.String
        )

        assertNull(queue.deadLetterQueue)
        assertNull(queue.archiveQueue)
    }

}
