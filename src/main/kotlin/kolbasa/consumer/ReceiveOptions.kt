package kolbasa.consumer

import kolbasa.queue.Checks
import kolbasa.queue.QueueOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.order.Order
import java.time.Duration

data class ReceiveOptions @JvmOverloads constructor(
    /**
     * Visibility timeout for this specific consume() call.
     * Visibility timeout is a delay before consumed but not deleted message will be
     * visible to another consumers. By default, value is not set, which means the
     * [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout] will be used or,
     * if it's not set too, the
     * [QueueOptions.defaultVisibilityTimeout][kolbasa.queue.QueueOptions.defaultVisibilityTimeout] will be used.
     *
     * Has the highest priority, next is
     * [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout] and, at the end,
     * [QueueOptions.defaultVisibilityTimeout][kolbasa.queue.QueueOptions.defaultVisibilityTimeout]
     */
    val visibilityTimeout: Duration = QueueOptions.VISIBILITY_TIMEOUT_NOT_SET,

    /**
     * Do we need to read metadata?
     * By default, we don't want to read these fields from the DB, parse them and instantiate the
     * meta values for performance reasons.
     *
     * You don't need to read metadata if the only thing you need is to filter (or sort) messages by metadata fields.
     */
    val readMetadata: Boolean = false,

    /**
     * If you want to receive messages in a specific order, you can specify it here.
     */
    val order: List<Order>? = null,

    /**
     * If you want to receive messages filtered by meta fields values, you can specify it here.
     */
    val filter: Condition? = null,
) {

    constructor(visibilityTimeout: Duration, readMetadata: Boolean, order: Order, filter: Condition?) :
        this(visibilityTimeout, readMetadata, listOf(order), filter)

    init {
        Checks.checkVisibilityTimeout(visibilityTimeout)
    }

    // do we need to read, parse and propagate OT data?
    internal var readOpenTelemetryData: Boolean = false

}


