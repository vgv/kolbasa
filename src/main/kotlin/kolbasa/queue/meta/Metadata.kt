package kolbasa.queue.meta

import kolbasa.queue.Checks
import kolbasa.schema.Const
import java.time.Instant

/**
 * Metadata for a queue.
 *
 * Metadata is a set of fields that will be stored in the database along with the message. Each message has
 * its own metadata values. Metadata is not required and can be empty.
 * It's useful if you want to filter messages by some fields or sort by them.
 *
 * Should you use metadata or store everything in the message body?
 * Please read the documentation for [Queue.metadata][kolbasa.queue.Queue.metadata] field
 *
 * ## Usage Example
 *
 * ```kotlin
 * val accountId = MetaField.ofLong("account_id", FieldOption.SEARCH)
 * val priority = MetaField.ofInt("priority", FieldOption.SEARCH)
 *
 * val queue = Queue.of("orders", PredefinedDataTypes.String, Metadata.of(accountId, priority))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var accountId = MetaField.ofLong("account_id", FieldOption.SEARCH);
 * var priority = MetaField.ofInt("priority", FieldOption.SEARCH);
 *
 * var queue = Queue.of("orders", PredefinedDataTypes.String, Metadata.of(accountId, priority));
 * ```
 */
data class Metadata(val fields: List<MetaField<*>>) {

    init {
        // All meta-fields must be unique. Attempting to declare multiple meta-fields with the same name is an indicator
        // of a serious library usage error and will result in an immediate exception
        // user_id, userId and USER_ID are the same database field name
        Checks.checkMetaFieldsUnique(fields)
    }

    private val nameToFields = fields.associateBy { it.name }

    /** Returns the declared field with this name, or `null` if the queue has no such field. */
    fun findByName(fieldName: String): MetaField<*>? = nameToFields[fieldName]

    companion object {

        /** Metadata of a queue that declares no meta fields, which is the default for [Queue][kolbasa.queue.Queue]. */
        @JvmField
        val EMPTY = of(emptyList())

        /** Declares the meta fields of a queue. Field names must be unique, otherwise the call throws. */
        @JvmStatic
        fun of(vararg fields: MetaField<*>) = of(fields.toList())

        /** Declares the meta fields of a queue. Field names must be unique, otherwise the call throws. */
        @JvmStatic
        fun of(fields: List<MetaField<*>>) = Metadata(fields)

        // --- DLQ original-value fields ---
        // Preserve: id, created_at, processing_at, scheduled_at
        // Use direct subclass constructors to bypass checkUserDefinedMetaFieldName (reserved suffix check)

        /** Original message `id bigint` from the source queue */
        @JvmField
        val DLQ_ORIGINAL_ID: MetaField<Long> =
            LongField("original_${Const.ID_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original `created_at timestamptz` from the source queue */
        @JvmField
        val DLQ_ORIGINAL_CREATED_AT: MetaField<Instant> =
            InstantField("original_${Const.CREATED_AT_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original `processing_at timestamptz` from the source queue */
        @JvmField
        val DLQ_ORIGINAL_PROCESSING_AT: MetaField<Instant> =
            InstantField("original_${Const.PROCESSING_AT_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original `scheduled_at timestamptz` from the source queue */
        @JvmField
        val DLQ_ORIGINAL_SCHEDULED_AT: MetaField<Instant> =
            InstantField("original_${Const.SCHEDULED_AT_COLUMN_NAME}${Const.DLQ_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /**
         * The four fields a [DLQ][kolbasa.queue.DlqOptions] adds to its own metadata. They keep the id and the
         * timestamps a message had in the source queue.
         *
         * A DLQ has every meta field of its parent queue plus these four.
         */
        @JvmField
        val DLQ_FIELDS = listOf(
            DLQ_ORIGINAL_ID,
            DLQ_ORIGINAL_CREATED_AT,
            DLQ_ORIGINAL_PROCESSING_AT,
            DLQ_ORIGINAL_SCHEDULED_AT
        )

        // --- Archive original-value fields ---
        // Preserve: id, created_at, remaining_attempts, processing_at
        // Use direct subclass constructors to bypass checkUserDefinedMetaFieldName (reserved suffix check)

        /** Original message `id bigint` from the source queue */
        @JvmField
        val ARCHIVE_ORIGINAL_ID: MetaField<Long> =
            LongField("original_${Const.ID_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original `created_at timestamptz` from the source queue */
        @JvmField
        val ARCHIVE_ORIGINAL_CREATED_AT: MetaField<Instant> =
            InstantField("original_${Const.CREATED_AT_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original `remaining_attempts int` from the source queue */
        @JvmField
        val ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS: MetaField<Int> =
            IntField("original_${Const.REMAINING_ATTEMPTS_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /** Original `processing_at timestamptz` from the source queue */
        @JvmField
        val ARCHIVE_ORIGINAL_PROCESSING_AT: MetaField<Instant> =
            InstantField("original_${Const.PROCESSING_AT_COLUMN_NAME}${Const.ARCHIVE_TABLE_NAME_SUFFIX}", FieldOption.NONE)

        /**
         * The four fields an [archive queue][kolbasa.queue.ArchiveQueueOptions] adds to its own metadata. They
         * keep the id, the timestamps and the attempts a message had in the source queue.
         *
         * An archive queue has every meta field of its parent queue plus these four.
         */
        @JvmField
        val ARCHIVE_FIELDS = listOf(
            ARCHIVE_ORIGINAL_ID,
            ARCHIVE_ORIGINAL_CREATED_AT,
            ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS,
            ARCHIVE_ORIGINAL_PROCESSING_AT
        )
    }
}


