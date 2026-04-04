package kolbasa.queue.meta

import kolbasa.schema.Const

/**
 * Metadata for a queue.
 *
 * Metadata is a set of fields that will be stored in the database along with the message. Each message has
 * its own metadata values. Metadata is not required and can be empty.
 * It's useful if you want to filter messages by some fields or sort by them.
 *
 * Should you use metadata or store everything in the message body?
 * Please read the documentation for [Queue.metadata][kolbasa.queue.Queue.metadata] field
 */
data class Metadata(val fields: List<MetaField<*>>) {

    private val nameToFields = fields.associateBy { it.name }

    fun findByName(fieldName: String): MetaField<*>? = nameToFields[fieldName]

    companion object {
        val EMPTY = of(emptyList())

        fun of(vararg fields: MetaField<*>) = of(fields.toList())
        fun of(fields: List<MetaField<*>>) = Metadata(fields)

        // --- DLQ original-value fields ---
        // Preserve: id, created_at, processing_at, scheduled_at
        // Use direct subclass constructors to bypass checkUserDefinedMetaFieldName (reserved suffix check)

        /** Original message id from the source queue */
        val DLQ_ORIGINAL_ID: MetaField<Long> =
            LongField("original_${Const.ID_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original created_at timestamp (epoch millis) from the source queue */
        val DLQ_ORIGINAL_CREATED_AT: MetaField<Long> =
            LongField("original_${Const.CREATED_AT_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original processing_at timestamp (epoch millis) from the source queue */
        val DLQ_ORIGINAL_PROCESSING_AT: MetaField<Long> =
            LongField("original_${Const.PROCESSING_AT_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original scheduled_at timestamp (epoch millis) from the source queue */
        val DLQ_ORIGINAL_SCHEDULED_AT: MetaField<Long> =
            LongField("original_${Const.SCHEDULED_AT_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        val DLQ_FIELDS = listOf(
            DLQ_ORIGINAL_ID,
            DLQ_ORIGINAL_CREATED_AT,
            DLQ_ORIGINAL_PROCESSING_AT,
            DLQ_ORIGINAL_SCHEDULED_AT
        )

        // --- Archive original-value fields ---
        // Preserve: id, created_at, remaining_attempts, processing_at
        // Use direct subclass constructors to bypass checkUserDefinedMetaFieldName (reserved suffix check)

        /** Original message id from the source queue */
        val ARCHIVE_ORIGINAL_ID: MetaField<Long> =
            LongField("original_${Const.ID_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original created_at timestamp (epoch millis) from the source queue */
        val ARCHIVE_ORIGINAL_CREATED_AT: MetaField<Long> =
            LongField("original_${Const.CREATED_AT_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original remaining_attempts from the source queue */
        val ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS: MetaField<Int> =
            IntField("original_${Const.REMAINING_ATTEMPTS_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original processing_at timestamp (epoch millis) from the source queue */
        val ARCHIVE_ORIGINAL_PROCESSING_AT: MetaField<Long> =
            LongField("original_${Const.PROCESSING_AT_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        val ARCHIVE_FIELDS = listOf(
            ARCHIVE_ORIGINAL_ID,
            ARCHIVE_ORIGINAL_CREATED_AT,
            ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS,
            ARCHIVE_ORIGINAL_PROCESSING_AT
        )
    }
}


