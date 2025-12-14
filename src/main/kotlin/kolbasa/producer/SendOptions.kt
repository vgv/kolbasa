package kolbasa.producer

import kolbasa.queue.Checks
import java.time.Duration
import java.util.concurrent.ExecutorService

data class SendOptions(
    /**
     * Delay before message will be visible to consumers.
     *
     * For example, if delay is 5 minutes, message will be visible to consumers after 5 minutes after sending. By default, the
     * value is unspecified (null), meaning that [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay] will be
     * used. The default value of [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay], in turn, is
     * [Duration.ZERO][java.time.Duration.ZERO], meaning that messages become available to consumers immediately after sending.
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
     * So, if you set a default delay of 10 minutes at the queue level, but a specific producer has a delay of 5 minutes using
     * [ProducerOptions.delay][kolbasa.producer.ProducerOptions.delay], messages sent by that producer (!) will be available to
     * consumers after 5 minutes. If, however, you set a delay of 2 minutes for a specific send() call using
     * [SendOptions.delay][kolbasa.producer.SendOptions.delay], messages sent in that call will be available after 2 minutes,
     * overriding both the producer and queue defaults. Finally, if you set a delay of 1 minute for a specific message using
     * [MessageOptions.delay][kolbasa.producer.MessageOptions.delay], that message will be available after 1 minute, overriding
     * send() call, producer and queue defaults.
     */
    val delay: Duration? = null,

    /**
     * Queue consume attempts before message will be expired or moved to DLQ.
     *
     * By default, the value is unspecified (null), meaning that
     * [QueueOptions.defaultAttempts][kolbasa.queue.QueueOptions.defaultAttempts] will be used. The default value of
     * [QueueOptions.defaultAttempts][kolbasa.queue.QueueOptions.defaultAttempts], in turn, is 5, meaning 5 attempts to
     * process this message before it becomes unavailable.
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
     * So, if you set a default attempts of 10 at the queue level, but a specific producer has attempts of 5 using
     * [ProducerOptions.attempts][kolbasa.producer.ProducerOptions.attempts], messages sent by that producer (!) will be expired
     * after 5 attempts. If, however, you set attempts of 3 for a specific send() call using
     * [SendOptions.attempts][kolbasa.producer.SendOptions.attempts], messages sent in that call will be expired after 3 attempts,
     * overriding both the producer and queue defaults. Finally, if you set attempts of 2 for a specific message using
     * [MessageOptions.attempts][kolbasa.producer.MessageOptions.attempts], that message will be expired after 2 attempts,
     * overriding send() call, producer and queue defaults.
     */
    val attempts: Int? = null,

    /**
     * An arbitrary sender name.
     * Each message sent using the send() call with the specified option will have this name in the 'producer' column
     * of the corresponding queue table.
     *
     * Used for debugging purposes only. There is no way to get this value during consuming.
     * If you feel it can be helpful to understand which send() call sent a message when you debug your application by
     * exploring queue table directly in PostgreSQL, you can set this value.
     */
    val producer: String? = null,

    /**
     * @see [ProducerOptions.deduplicationMode]
     */
    val deduplicationMode: DeduplicationMode? = null,

    /**
     * @see [ProducerOptions.batchSize]
     */
    val batchSize: Int? = null,

    /**
     * @see [ProducerOptions.partialInsert]
     */
    val partialInsert: PartialInsert? = null,

    /**
     * @see [ProducerOptions.shard]
     */
    val shard: Int? = null,

    /**
     * @see [ProducerOptions.asyncExecutor]
     */
    val asyncExecutor: ExecutorService? = null,
) {

    init {
        Checks.checkDelay(delay)
        Checks.checkAttempts(attempts)
        Checks.checkProducerName(producer)
        Checks.checkBatchSize(batchSize)
    }

    class Builder internal constructor() {
        private var delay: Duration? = null
        private var attempts: Int? = null
        private var producer: String? = null
        private var deduplicationMode: DeduplicationMode? = null
        private var batchSize: Int? = null
        private var partialInsert: PartialInsert? = null
        private var shard: Int? = null
        private var asyncExecutor: ExecutorService? = null

        fun delay(delay: Duration) = apply { this.delay = delay }
        fun attempts(attempts: Int) = apply { this.attempts = attempts }
        fun producer(producer: String) = apply { this.producer = producer }
        fun deduplicationMode(deduplicationMode: DeduplicationMode) = apply { this.deduplicationMode = deduplicationMode }
        fun batchSize(batchSize: Int) = apply { this.batchSize = batchSize }
        fun partialInsert(partialInsert: PartialInsert) = apply { this.partialInsert = partialInsert }
        fun shard(shard: Int) = apply { this.shard = shard }
        fun asyncExecutor(asyncExecutor: ExecutorService) = apply { this.asyncExecutor = asyncExecutor }

        fun build() = SendOptions(
            delay = delay,
            attempts = attempts,
            producer = producer,
            deduplicationMode = deduplicationMode,
            batchSize = batchSize,
            partialInsert = partialInsert,
            shard = shard,
            asyncExecutor = asyncExecutor
        )
    }

    companion object {
        internal val DEFAULT = SendOptions()

        @JvmStatic
        fun builder() = Builder()
    }

}
