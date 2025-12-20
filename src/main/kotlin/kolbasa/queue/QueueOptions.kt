package kolbasa.queue

import java.time.Duration

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
     * Default queue visibility timeout. Delay before consumed but not deleted message will be visible to another
     * consumers. Default value is 60 seconds.
     *
     * Can be overridden by [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout] for
     * a specific consumer or by [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout]
     * for every consume() call. [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout]
     * has the highest priority, next is
     * [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout] and, at the end,
     * [defaultVisibilityTimeout]
     */
    val defaultVisibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT
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

        fun defaultDelay(defaultDelay: Duration) = apply { this.defaultDelay = defaultDelay }
        fun defaultAttempts(defaultAttempts: Int) = apply { this.defaultAttempts = defaultAttempts }
        fun defaultVisibilityTimeout(defaultVisibilityTimeout: Duration) = apply { this.defaultVisibilityTimeout = defaultVisibilityTimeout }

        fun build() = QueueOptions(defaultDelay, defaultAttempts, defaultVisibilityTimeout)
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
