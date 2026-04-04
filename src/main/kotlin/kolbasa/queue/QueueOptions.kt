package kolbasa.queue

import java.time.Duration

/**
 * Global queue options
 *
 * Default options for a queue, which can be overridden at various levels, from producer and consumer options
 * to individual `send()` and `receive()` calls, and finally to individual messages.
 *
 * For more details on how options can be overridden, see the documentation for these classes:
 * 1. [ProducerOptions][kolbasa.producer.ProducerOptions]
 * 2. [ConsumerOptions][kolbasa.consumer.ConsumerOptions]
 * 3. [SendOptions][kolbasa.producer.SendOptions]
 * 4. [MessageOptions][kolbasa.producer.MessageOptions]
 * 5. [ReceiveOptions][kolbasa.consumer.ReceiveOptions]
 */
data class QueueOptions(
    /**
     * Default queue delay, before message will be visible to consumers.
     *
     * For example, if delay is 5 minutes, message will be visible to consumers after 5 minutes after sending.
     * The default value is [Duration.ZERO][java.time.Duration.ZERO] which means that messages are available to consumers
     * immediately after sending.
     *
     * The value can be overridden at various levels, from a global queue-wide setting to values for specific producers,
     * specific send() calls, and, finally, the most granular level – individual messages.
     *
     * Values can be overridden in this order, from lowest to highest priority:
     * 1. [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay]  (lowest priority)
     * 2. [ProducerOptions.delay][kolbasa.producer.ProducerOptions.delay]
     * 3. [SendOptions.delay][kolbasa.producer.SendOptions.delay]
     * 4. [MessageOptions.delay][kolbasa.producer.MessageOptions.delay] (highest priority)
     *
     * So, if you set a default delay of 10 minutes at the queue level, but a specific producer has a delay of 5 minutes,
     * messages sent by that producer (!) will be available to consumers after 5 minutes. If, however, you set a delay of 2 minutes
     * for a specific send() call using [SendOptions.delay][kolbasa.producer.SendOptions.delay], messages sent in that call will
     * be available after 2 minutes, overriding both the producer and queue defaults. Finally, if you set a delay of 1 minute
     * for a specific message using [MessageOptions.delay][kolbasa.producer.MessageOptions.delay], that message will be available
     * after 1 minute, overriding send() call, producer and queue defaults.
     */
    val defaultDelay: Duration = DEFAULT_DELAY,

    /**
     * Default queue consume attempts before message will be expired or moved to DLQ.
     *
     * The default value is 5, which means five attempts to process this message before it becomes unavailable.
     *
     * The value can be overridden at various levels, from a global queue-wide setting to values for specific producers,
     * specific send() calls, and, finally, the most granular level – individual messages.
     *
     * Values can be overridden in this order, from lowest to highest priority:
     * 1. [QueueOptions.defaultAttempts][kolbasa.queue.QueueOptions.defaultAttempts]  (lowest priority)
     * 2. [ProducerOptions.attempts][kolbasa.producer.ProducerOptions.attempts]
     * 3. [SendOptions.attempts][kolbasa.producer.SendOptions.attempts]
     * 4. [MessageOptions.attempts][kolbasa.producer.MessageOptions.attempts] (highest priority)
     *
     * So, if you set a default attempts of 10 at the queue level, but a specific producer has attempts of 5,
     * messages sent by that producer (!) will be expired after 5 attempts. If, however, you set attempts of 3
     * for a specific send() call using [SendOptions.attempts][kolbasa.producer.SendOptions.attempts], messages sent in that
     * call will be expired after 3 attempts, overriding both the producer and queue defaults. Finally, if you set attempts of 2
     * for a specific message using [MessageOptions.attempts][kolbasa.producer.MessageOptions.attempts], that message will be
     * expired after 2 attempts, overriding send() call, producer and queue defaults.
     */
    val defaultAttempts: Int = DEFAULT_ATTEMPTS,

    /**
     * Default queue visibility timeout.
     *
     * Delay before consumed but not deleted message will be visible to another consumers. Default value is 60 seconds.
     *
     * The value can be overridden at various levels, from a global queue-wide setting to values for specific consumers
     * and, finally, the most granular level – individual receive() calls.
     *
     * Values can be overridden in this order, from lowest to highest priority:
     * 1. [QueueOptions.defaultVisibilityTimeout][kolbasa.queue.QueueOptions.defaultVisibilityTimeout]  (lowest priority)
     * 2. [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout]
     * 3. [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout] (highest priority)
     *
     * So, if you set the default timeout to 10 minutes at the queue level, but a specific consumer has a timeout of 5
     * minutes using [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout], messages received
     * by that consumer (!) will be visible in the queue again after 5 minutes. However, if you set a timeout of 2 minutes for
     * a specific receive() call using [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout],
     * messages received in that call will be available after 2 minutes, overriding the default values for both the consumer
     * and the queue.
     */
    val defaultVisibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,

    /**
     * Dead Letter Queue (DLQ) configuration — a companion queue for **failed** messages.
     *
     * When non-null, a companion DLQ table is created alongside the main queue table.
     * Messages that exhaust all processing attempts (`remaining_attempts` reaches 0) are atomically
     * moved to the DLQ during the sweep cycle instead of being permanently deleted.
     * This allows failed messages to be inspected, debugged, or reprocessed later.
     *
     * The DLQ is a regular [Queue] accessible via [Queue.deadLetterQueue] and works with all existing
     * APIs — `Consumer`, `Producer`, `Inspector`, `Mutator`.
     *
     * Note: DLQ is for failed messages, Archive is for successfully processed ones.
     *
     * Default: `null` (DLQ disabled, expired messages are deleted).
     *
     * @see DlqOptions
     * @see Queue.deadLetterQueue
     * @see archiveQueueOptions
     */
    val dlqOptions: DlqOptions? = null,

    /**
     * Archive queue configuration — a companion queue for **successfully processed** messages.
     *
     * When non-null, a companion Archive table is created alongside the main queue table.
     * When a consumer deletes a message after successful processing, the message is atomically
     * moved to the Archive queue instead of being permanently deleted.
     * This is useful for auditing, compliance, trailing, or replaying successfully processed messages.
     *
     * The Archive queue is a regular [Queue] accessible via [Queue.archiveQueue] and works with all
     * existing APIs — `Consumer`, `Producer`, `Inspector`, `Mutator`.
     *
     * Note: DLQ is for failed messages, Archive is for successfully processed ones.
     *
     * Default: `null` (Archive disabled, deleted messages are permanently removed).
     *
     * @see ArchiveQueueOptions
     * @see Queue.archiveQueue
     * @see dlqOptions
     */
    val archiveQueueOptions: ArchiveQueueOptions? = null
) {

    init {
        Checks.checkDelay(defaultDelay)
        Checks.checkAttempts(defaultAttempts)
        Checks.checkVisibilityTimeout(defaultVisibilityTimeout)
    }

    class Builder internal constructor() {
        private var defaultDelay: Duration = DEFAULT_DELAY
        private var defaultAttempts: Int = DEFAULT_ATTEMPTS
        private var defaultVisibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT
        private var dlqOptions: DlqOptions? = null
        private var archiveQueueOptions: ArchiveQueueOptions? = null

        fun defaultDelay(defaultDelay: Duration) = apply { this.defaultDelay = defaultDelay }
        fun defaultAttempts(defaultAttempts: Int) = apply { this.defaultAttempts = defaultAttempts }
        fun defaultVisibilityTimeout(defaultVisibilityTimeout: Duration) =
            apply { this.defaultVisibilityTimeout = defaultVisibilityTimeout }

        fun enableDlq(dlqOptions: DlqOptions = DlqOptions.DEFAULT) = apply { this.dlqOptions = dlqOptions }
        fun enableArchiveQueue(archiveQueueOptions: ArchiveQueueOptions = ArchiveQueueOptions.DEFAULT) =
            apply { this.archiveQueueOptions = archiveQueueOptions }

        fun build() = QueueOptions(defaultDelay, defaultAttempts, defaultVisibilityTimeout, dlqOptions, archiveQueueOptions)
    }

    companion object {
        private val DEFAULT_DELAY = Duration.ZERO
        private const val DEFAULT_ATTEMPTS = 5
        private val DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(60)

        val DEFAULT = QueueOptions()

        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
