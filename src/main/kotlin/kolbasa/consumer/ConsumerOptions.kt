package kolbasa.consumer

import kolbasa.queue.Checks
import kolbasa.queue.QueueOptions
import java.time.Duration

data class ConsumerOptions(
    val consumer: String? = null,
    val visibilityTimeout: Duration = QueueOptions.VISIBILITY_TIMEOUT_NOT_SET
) {

    init {
        Checks.checkConsumerName(consumer)
        Checks.checkVisibilityTimeout(visibilityTimeout)
    }

}
