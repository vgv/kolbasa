package kolbasa.queue.meta

import kolbasa.queue.*
import kolbasa.schema.Const
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MetadataTest {

    private val USER_ID = MetaField.int("user_id")

    @Test
    fun testDlqFieldsCount() {
        assertEquals(4, Metadata.DLQ_FIELDS.size)
    }

    @Test
    fun testArchiveFieldsCount() {
        assertEquals(4, Metadata.ARCHIVE_FIELDS.size)
    }

    @Test
    fun testDlqFieldsSuffixes() {
        Metadata.DLQ_FIELDS.forEach { field ->
            assertTrue(
                field.name.endsWith(Const.DLQ_TABLE_NAME_SUFFIX),
                "DLQ field '${field.name}' should end with '${Const.DLQ_TABLE_NAME_SUFFIX}'"
            )
        }
    }

    @Test
    fun testArchiveFieldsSuffixes() {
        Metadata.ARCHIVE_FIELDS.forEach { field ->
            assertTrue(
                field.name.endsWith(Const.ARCHIVE_TABLE_NAME_SUFFIX),
                "Archive field '${field.name}' should end with '${Const.ARCHIVE_TABLE_NAME_SUFFIX}'"
            )
        }
    }

    @Test
    fun testDlqFieldsHaveNoOption() {
        Metadata.DLQ_FIELDS.forEach { field ->
            assertEquals(FieldOption.NONE, field.option, "DLQ field '${field.name}' should have FieldOption.NONE")
        }
    }

    @Test
    fun testArchiveFieldsHaveNoOption() {
        Metadata.ARCHIVE_FIELDS.forEach { field ->
            assertEquals(FieldOption.NONE, field.option, "Archive field '${field.name}' should have FieldOption.NONE")
        }
    }

    @Test
    fun testDlqCompanionQueueHasCorrectFields() {
        val queue = Queue(
            name = "test",
            databaseDataType = PredefinedDataTypes.String,
            metadata = Metadata.of(USER_ID),
            options = QueueOptions(dlqOptions = DlqOptions.DEFAULT)
        )

        val dlq = requireNotNull(queue.deadLetterQueue)
        // User field (stripped) + 4 DLQ original fields
        assertEquals(1 + 4, dlq.metadata.fields.size)
        // User field should have NONE option
        assertEquals(FieldOption.NONE, dlq.metadata.findByName(USER_ID.name)?.option)
        // DLQ fields should be present
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_ID.name))
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_CREATED_AT.name))
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_PROCESSING_AT.name))
        assertNotNull(dlq.metadata.findByName(Metadata.DLQ_ORIGINAL_SCHEDULED_AT.name))
    }

    @Test
    fun testArchiveCompanionQueueHasCorrectFields() {
        val queue = Queue(
            name = "test",
            databaseDataType = PredefinedDataTypes.String,
            metadata = Metadata.of(USER_ID),
            options = QueueOptions(archiveQueueOptions = ArchiveQueueOptions.DEFAULT)
        )

        val archive = requireNotNull(queue.archiveQueue)
        // User field (stripped) + 4 Archive original fields
        assertEquals(1 + 4, archive.metadata.fields.size)
        // User field should have NONE option
        assertEquals(FieldOption.NONE, archive.metadata.findByName(USER_ID.name)?.option)
        // Archive fields should be present
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_ID.name))
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_CREATED_AT.name))
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS.name))
        assertNotNull(archive.metadata.findByName(Metadata.ARCHIVE_ORIGINAL_PROCESSING_AT.name))
    }
}
