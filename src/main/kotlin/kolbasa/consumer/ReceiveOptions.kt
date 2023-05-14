package kolbasa.consumer

import kolbasa.queue.Checks
import kolbasa.queue.QueueOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.order.Order
import java.time.Duration

data class ReceiveOptions<M : Any> @JvmOverloads constructor(
    val visibilityTimeout: Duration = QueueOptions.VISIBILITY_TIMEOUT_NOT_SET,
    val order: List<Order<M>>? = null,
    val filter: Condition<M>? = null,
) {

    constructor(visibilityTimeout: Duration, order: Order<M>, filter: Condition<M>?) :
        this(visibilityTimeout, listOf(order), filter)

    init {
        Checks.checkVisibilityTimeout(visibilityTimeout)
    }

}


