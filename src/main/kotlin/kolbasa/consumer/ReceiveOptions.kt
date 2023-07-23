package kolbasa.consumer

import kolbasa.queue.Checks
import kolbasa.queue.QueueOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.order.Order
import java.time.Duration

data class ReceiveOptions<M : Any> @JvmOverloads constructor(
    val visibilityTimeout: Duration = QueueOptions.VISIBILITY_TIMEOUT_NOT_SET,
    /**
     * Do we need to read metadata?
     * By default, we don't want to read these fields from the DB, parse them and instantiate the meta-class M
     */
    val readMetadata: Boolean = false,
    val order: List<Order<M>>? = null,
    val filter: Condition<M>? = null,
) {

    constructor(visibilityTimeout: Duration, readMetadata: Boolean, order: Order<M>, filter: Condition<M>?) :
        this(visibilityTimeout, readMetadata, listOf(order), filter)

    init {
        Checks.checkVisibilityTimeout(visibilityTimeout)
    }

}


