package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.order.Order
import kolbasa.queue.Checks
import java.time.Duration

data class ReceiveOptions @JvmOverloads constructor(
    /**
     * Arbitrary consumer name.
     *
     * Every message, consumed by receive() call with the specified option will have this name in the 'consumer' column
     * of corresponding queue table.
     *
     * Used for debugging purposes only. There is no way to get this value during consuming.
     * If you feel it can be helpful to understand which consumer read a message when you debug your application by
     * exploring queue table directly in PostgreSQL, you can set this value.
     */
    val consumer: String? = null,

    /**
     * Visibility timeout for this specific consume() call.
     *
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
    val visibilityTimeout: Duration? = null,

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

    init {
        Checks.checkConsumerName(consumer)
        Checks.checkVisibilityTimeout(visibilityTimeout)
    }

    // do we need to read, parse and propagate OT data?
    internal var readOpenTelemetryData: Boolean = false

    class Builder {
        private var consumer: String? = null
        private var visibilityTimeout: Duration? = null
        private var readMetadata: Boolean = false
        private var order: List<Order>? = null
        private var filter: Condition? = null

        fun consumer(consumer: String) = apply { this.consumer = consumer }
        fun visibilityTimeout(visibilityTimeout: Duration) = apply { this.visibilityTimeout = visibilityTimeout }
        fun readMetadata(readMetadata: Boolean) = apply { this.readMetadata = readMetadata }
        fun order(order: List<Order>) = apply { this.order = order }
        fun filter(filter: Condition) = apply { this.filter = filter }

        fun build(): ReceiveOptions {
            return ReceiveOptions(
                consumer = consumer,
                visibilityTimeout = visibilityTimeout,
                readMetadata = readMetadata,
                order = order,
                filter = filter
            )
        }
    }

    companion object {

        internal val DEFAULT = ReceiveOptions()

        @JvmStatic
        fun builder() = Builder()
    }

}


