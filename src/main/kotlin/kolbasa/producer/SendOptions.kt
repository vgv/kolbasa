package kolbasa.producer

import kolbasa.queue.Checks
import kolbasa.queue.QueueOptions
import java.time.Duration

data class SendOptions @JvmOverloads constructor(
    val delay: Duration = QueueOptions.DELAY_NOT_SET,
    val attempts: Int = QueueOptions.ATTEMPTS_NOT_SET
) {

    init {
        Checks.checkDelay(delay)
        Checks.checkAttempts(attempts)
    }

}
