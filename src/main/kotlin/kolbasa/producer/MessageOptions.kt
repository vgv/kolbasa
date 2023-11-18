package kolbasa.producer

import kolbasa.queue.Checks
import kolbasa.queue.QueueOptions
import java.time.Duration

data class MessageOptions @JvmOverloads constructor(
    /**
     * Delay before message will be visible to consumers.
     *
     * For example, if delay is 5 minutes, message will be visible to consumers after 5 minutes after sending. By default,
     * value is not set, which means the [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay] will be used.
     *
     * Can be configured by [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay] at the queue level, but
     * this value has priority over [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay], if configured.
     */
    val delay: Duration = QueueOptions.DELAY_NOT_SET,

    /**
     * Default queue consume attempts before message will be expired or moved to DLQ. By default, value is not set,
     * which means the [QueueOptions.defaultAttempts][kolbasa.queue.QueueOptions.defaultAttempts] will be used.
     *
     * Can be configured by [QueueOptions.defaultAttempts][kolbasa.queue.QueueOptions.defaultAttempts] at the queue
     * level, but this value has priority over
     * [QueueOptions.defaultAttempts][kolbasa.queue.QueueOptions.defaultAttempts], if configured.
     */
    val attempts: Int = QueueOptions.ATTEMPTS_NOT_SET
) {

    init {
        Checks.checkDelay(delay)
        Checks.checkAttempts(attempts)
    }

}
