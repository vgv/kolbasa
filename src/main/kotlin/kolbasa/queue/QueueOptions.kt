package kolbasa.queue

import java.time.Duration

data class QueueOptions @JvmOverloads constructor(
    /**
     * Default queue delay, before message will be visible to consumers.
     *
     * For example, if delay is 5 minutes, message will be visible to consumers after 5 minutes after sending.
     * By default, messages are available to consumers immediately after sending.
     *
     * Can be overridden by [SendOptions.delay][kolbasa.producer.SendOptions.delay] for every send() call.
     * [SendOptions.delay][kolbasa.producer.SendOptions.delay] has priority over [defaultDelay]
     */
    val defaultDelay: Duration = DEFAULT_DELAY,
    /**
     * Default queue consume attempts before message will be expired or moved to DLQ. Default value is 5.
     *
     * Can be overridden by [SendOptions.attempts][kolbasa.producer.SendOptions.attempts] for every send() call.
     * [SendOptions.attempts][kolbasa.producer.SendOptions.attempts] has priority over [defaultAttempts]
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

    internal companion object {
        internal val DEFAULT_DELAY = Duration.ZERO
        internal val DELAY_NOT_SET = Duration.ofMillis(-1)

        internal const val DEFAULT_ATTEMPTS = 5
        internal const val ATTEMPTS_NOT_SET = -1

        internal val DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(60)
        internal val VISIBILITY_TIMEOUT_NOT_SET = Duration.ofMillis(-1)
    }
}
