package kolbasa.queue

import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.Metadata
import kolbasa.schema.Const
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration

class QueueTest {

    private val USER_ID = MetaField.ofInt("user_id")
    private val NAME = MetaField.ofString("name")

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
        assertEquals(QueueRole.DLQ, dlq.role)
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
        assertEquals(QueueRole.ARCHIVE, archive.role)
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

    @Test
    fun testWithName_KeepsTheRest() {
        val options = QueueOptions(defaultAttempts = 42, dlqOptions = DlqOptions.DEFAULT)
        val queue = Queue("orders", PredefinedDataTypes.String, options, Metadata.of(USER_ID, NAME))

        val renamed = queue.withName("orders_outbox")

        assertEquals(Queue("orders_outbox", PredefinedDataTypes.String, options, Metadata.of(USER_ID, NAME)), renamed)
        assertEquals("orders_outbox${Const.DLQ_TABLE_NAME_SUFFIX}", renamed.deadLetterQueue?.name)

        // The original stays as it was
        assertEquals("orders", queue.name)
    }

    @Test
    fun testWithOptions_ReplacesAllOptions() {
        val queue = Queue(
            name = "orders",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(defaultDelay = Duration.ofSeconds(10), defaultAttempts = 42),
            metadata = Metadata.of(USER_ID, NAME)
        )

        val newOptions = QueueOptions(defaultAttempts = 10_000)
        val changed = queue.withOptions(newOptions)

        // Not merged: the delay of the original is gone
        assertEquals(Queue("orders", PredefinedDataTypes.String, newOptions, Metadata.of(USER_ID, NAME)), changed)

        // The original stays as it was
        assertEquals(42, queue.options.defaultAttempts)
    }

    @Test
    fun testWithName_ChecksTheName() {
        val queue = Queue.of("orders", PredefinedDataTypes.String)

        assertThrows(IllegalArgumentException::class.java) {
            queue.withName("orders${Const.DLQ_TABLE_NAME_SUFFIX}")
        }
    }

    @Test
    fun testCompanionQueue_CannotBeCopied() {
        val queue = Queue(
            name = "orders",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(dlqOptions = DlqOptions.DEFAULT)
        )
        val dlq = requireNotNull(queue.deadLetterQueue)

        assertThrows(IllegalStateException::class.java) {
            dlq.withName("other${Const.DLQ_TABLE_NAME_SUFFIX}")
        }
        assertThrows(IllegalStateException::class.java) {
            dlq.withOptions(QueueOptions.DEFAULT)
        }
    }
}
