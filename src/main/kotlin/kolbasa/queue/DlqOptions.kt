package kolbasa.queue

import java.time.Duration

/**
 * Configuration for a Dead Letter Queue (DLQ) attached to a [MAIN][QueueType.MAIN] queue.
 *
 * When a message exhausts all processing attempts (`remaining_attempts` reaches 0), the next
 * sweep cycle moves it atomically to the DLQ instead of permanently deleting it. Since sweep
 * is probabilistic, the move may not happen immediately but will occur eventually on a subsequent
 * [receive()][kolbasa.consumer.datasource.Consumer.receive] call. This allows failed messages
 * to be inspected, debugged, or reprocessed later.
 *
 * Retention controls how long messages stay in the DLQ before being cleaned up:
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
 *         dlqOptions = DlqOptions(
 *             retention = Duration.ofDays(14),
 *             maxMessages = 100_000
 *         )
 *     )
 * )
 * ```
 *
 * @see QueueOptions.dlqOptions
 * @see ArchiveQueueOptions
 * @see kolbasa.consumer.sweep.SweepConfig
 */
data class DlqOptions(
    /**
     * How long to retain messages in DLQ before cleanup.
     * Must be between [MIN_RETENTION] (1 hour) and [MAX_RETENTION] (10 years).
     * Default: 30 days.
     */
    val retention: Duration = Duration.ofDays(30),

    /**
     * Approximate maximum number of messages to retain in the DLQ.
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
        Checks.checkDlqRetention(retention)
        Checks.checkRetentionMaxMessages(maxMessages)
    }

    companion object {
        val MIN_RETENTION: Duration = Duration.ofHours(1)
        val MAX_RETENTION: Duration = Duration.ofDays(365 * 10L) // 10 years

        val DEFAULT = DlqOptions()
    }
}
