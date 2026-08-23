package kolbasa.producer

import kolbasa.queue.Checks
import java.time.Duration
import java.util.concurrent.ExecutorService

/**
 * Configuration options for a single `send()` call.
 *
 * SendOptions allows overriding [ProducerOptions] for a specific `send()` invocation.
 * This is useful when you need different behavior for certain batches of messages
 * without creating multiple producer instances.
 *
 * ## Options Hierarchy
 *
 * Kolbasa uses a layered configuration system where more specific settings override general ones.
 * For producer-related settings (`delay`, `attempts`, etc.), the priority order is:
 *
 * ```
 * QueueOptions (lowest) → ProducerOptions → SendOptions → MessageOptions (highest)
 * ```
 *
 * For example, if [ProducerOptions] sets `delay = 5 min` and SendOptions sets `delay = 2 min`,
 * messages in this specific `send()` call will use the 2-minute delay.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = SendOptions(
 *     delay = Duration.ofMinutes(2),
 *     attempts = 10,
 *     shard = 42
 * )
 *
 * // Use options in send() call
 * producer.send(queue, SendRequest(data = messages, sendOptions = options))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var options = SendOptions.builder()
 *     .delay(Duration.ofMinutes(2))
 *     .attempts(10)
 *     .shard(42)
 *     .build();
 *
 * // Use options in send() call
 * producer.send(queue, new SendRequest<>(messages, options));
 * ```
 *
 * @see ProducerOptions for producer-level defaults
 * @see MessageOptions for per-message overrides (highest priority)
 * @see kolbasa.queue.QueueOptions for queue-wide defaults
 */
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

    /** Builder for flexible [SendOptions] creation, when only some of the properties need to be set. */
    class Builder internal constructor() {
        private var delay: Duration? = null
        private var attempts: Int? = null
        private var producer: String? = null
        private var deduplicationMode: DeduplicationMode? = null
        private var batchSize: Int? = null
        private var partialInsert: PartialInsert? = null
        private var shard: Int? = null
        private var asyncExecutor: ExecutorService? = null

        /** Sets [SendOptions.delay] – how long the messages of this send() call stay invisible to consumers. */
        fun delay(delay: Duration) = apply { this.delay = delay }

        /** Sets [SendOptions.attempts] – how many times a message may be consumed before it expires or goes to a DLQ. */
        fun attempts(attempts: Int) = apply { this.attempts = attempts }

        /** Sets [SendOptions.producer] – the sender name written into the queue's `producer` column, for debugging. */
        fun producer(producer: String) = apply { this.producer = producer }

        /** Sets [SendOptions.deduplicationMode] – whether a duplicate key fails the send or is silently skipped. */
        fun deduplicationMode(deduplicationMode: DeduplicationMode) = apply { this.deduplicationMode = deduplicationMode }

        /** Sets [SendOptions.batchSize] – how many messages go into a single INSERT statement. */
        fun batchSize(batchSize: Int) = apply { this.batchSize = batchSize }

        /** Sets [SendOptions.partialInsert] – what happens to the rest of the batch when one chunk fails. */
        fun partialInsert(partialInsert: PartialInsert) = apply { this.partialInsert = partialInsert }

        /** Sets [SendOptions.shard] – the value that keeps messages with the same shard on the same cluster server. */
        fun shard(shard: Int) = apply { this.shard = shard }

        /** Sets [SendOptions.asyncExecutor] – the executor used by the producer's sendAsync() methods. */
        fun asyncExecutor(asyncExecutor: ExecutorService) = apply { this.asyncExecutor = asyncExecutor }

        /** Creates a new [SendOptions] instance: a fresh one on every call, nothing is cached or reused. */
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

        /** Default options: they override nothing and leave the default behaviour in place. */
        @JvmField
        val DEFAULT = SendOptions()

        @JvmStatic
        fun builder() = Builder()
    }

}
