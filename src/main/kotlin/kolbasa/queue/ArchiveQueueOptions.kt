package kolbasa.queue

import java.time.Duration

/**
 * Configuration for an Archive queue attached to a [MAIN][QueueType.MAIN] queue.
 *
 * When a consumer calls [delete()][kolbasa.consumer.datasource.Consumer.delete] on a message,
 * the message is atomically moved to the Archive queue instead of being permanently deleted.
 * This is useful for auditing, compliance, trailing, or replaying successfully processed messages.
 *
 * Retention controls how long messages stay in the Archive before being cleaned up:
 * - [retention] — duration-based: messages older than this are deleted
 * - [maxMessages] — count-based (approximate): keeps roughly this many messages, removing the oldest.
 *   The count is estimated using different PostgreSQL tricks to get an estimated table rows count
 *   rather than an exact `count(*)` to avoid a sequential scan on large tables.
 *
 * Retention cleanup runs during the probabilistic sweep cycle of the parent queue.
 *
 * ## Usage
 *
 * ```kotlin
 * val queue = Queue(
 *     name = "orders",
 *     databaseDataType = PredefinedDataTypes.String,
 *     options = QueueOptions(
 *         archiveQueueOptions = ArchiveQueueOptions(
 *             retention = Duration.ofDays(90),
 *             maxMessages = 1_000_000
 *         )
 *     )
 * )
 * ```
 *
 * @see QueueOptions.archiveQueueOptions
 * @see DlqOptions
 */
data class ArchiveQueueOptions(
    /**
     * How long to retain messages in Archive queue before cleanup.
     * Must be between [MIN_RETENTION] (1 hour) and [MAX_RETENTION] (10 years).
     * Default: 30 days.
     */
    val retention: Duration = Duration.ofDays(30),

    /**
     * Approximate maximum number of messages to retain in the Archive queue.
     * When this limit is exceeded, the oldest messages are removed first.
     *
     * The row count is estimated using different PostgreSQL tricks to get an estimated table rows
     * count rather than an exact `count(*)`, which requires a full sequential scan. This makes
     * enforcement approximate: the actual number of retained messages may temporarily exceed or
     * fall slightly below the configured limit, depending on how recently `VACUUM` or `ANALYZE`
     * has refreshed the table statistics.
     *
     * Default: null (no limit, retention by duration only).
     */
    val maxMessages: Long? = null
) {
    init {
        Checks.checkArchiveQueueRetention(retention)
        Checks.checkRetentionMaxMessages(maxMessages)
    }

    companion object {
        val MIN_RETENTION: Duration = Duration.ofHours(1)
        val MAX_RETENTION: Duration = Duration.ofDays(365 * 10L) // 10 years

        val DEFAULT = ArchiveQueueOptions()
    }
}
