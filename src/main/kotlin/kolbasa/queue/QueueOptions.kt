package kolbasa.queue

import java.time.Duration

data class QueueOptions @JvmOverloads constructor(
    val defaultDelay: Duration = DEFAULT_DELAY,
    val defaultAttempts: Int = DEFAULT_ATTEMPTS,
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
