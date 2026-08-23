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
 * The same from Java:
 *
 * ```java
 * var archiveQueueOptions = ArchiveQueueOptions.builder()
 *     .retention(Duration.ofDays(90))
 *     .maxMessages(1_000_000)
 *     .build();
 *
 * var queue = Queue.builder("orders", PredefinedDataTypes.String)
 *     .options(QueueOptions.builder().enableArchiveQueue(archiveQueueOptions).build())
 *     .build();
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
    val retention: Duration = DEFAULT_RETENTION,

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

    /** Builder for flexible [ArchiveQueueOptions] creation, when only some of the properties need to be set. */
    class Builder internal constructor() {
        private var retention: Duration = DEFAULT_RETENTION
        private var maxMessages: Long? = null

        /** Sets [ArchiveQueueOptions.retention] – how long a message is kept before the sweep cycle deletes it. */
        fun retention(retention: Duration): Builder = apply { this.retention = retention }

        /** Sets [ArchiveQueueOptions.maxMessages] – the approximate number of messages to keep, oldest removed first. */
        fun maxMessages(maxMessages: Long): Builder = apply { this.maxMessages = maxMessages }

        /** Creates a new [ArchiveQueueOptions] instance: a fresh one on every call, nothing is cached or reused. */
        fun build() = ArchiveQueueOptions(retention, maxMessages)
    }

    companion object {
        /** The smallest retention Kolbasa accepts – 1 hour. */
        @JvmField
        val MIN_RETENTION: Duration = Duration.ofHours(1)

        /** The retention used when none is set – 30 days. */
        @JvmField
        val DEFAULT_RETENTION: Duration = Duration.ofDays(30)

        /** The largest retention Kolbasa accepts – 10 years. */
        @JvmField
        val MAX_RETENTION: Duration = Duration.ofDays(365 * 10L) // 10 years

        /** Default options: 30 days retention and no message-count limit. */
        @JvmField
        val DEFAULT = ArchiveQueueOptions()

        @JvmStatic
        fun builder() = Builder()
    }
}
