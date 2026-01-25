package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.order.Order
import kolbasa.queue.Checks
import java.time.Duration

/**
 * Configuration options for a single `receive()` call.
 *
 * ReceiveOptions allows overriding [ConsumerOptions] for a specific `receive()` invocation
 * and provides additional capabilities like filtering and ordering that are only available
 * at the receive level, not at the consumer level.
 *
 * ## Options Hierarchy
 *
 * Kolbasa uses a layered configuration system where more specific settings override general ones.
 * For consumer-related settings, the priority order is:
 *
 * ```
 * QueueOptions (lowest) → ConsumerOptions → ReceiveOptions (highest)
 * ```
 *
 * ReceiveOptions has the highest priority for consumer settings.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = ReceiveOptions(
 *     visibilityTimeout = Duration.ofMinutes(2),
 *     readMetadata = true,
 *     filter = ACCOUNT_ID eq 123,
 *     order = PRIORITY.desc()
 * )
 *
 * // Use options in receive() call
 * val messages = consumer.receive(queue, 10, options)
 * ```
 *
 * @see ConsumerOptions for consumer-level defaults
 * @see kolbasa.queue.QueueOptions for queue-wide defaults
 * @see Condition for filtering options
 * @see Order for ordering options
 */
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
     * Visibility timeout
     *
     * Delay before consumed but not deleted message will be visible to another consumers. Default value is 60 seconds.
     *
     * By default, the value is unspecified (null), meaning that
     * [QueueOptions.defaultVisibilityTimeout][kolbasa.queue.QueueOptions.defaultVisibilityTimeout] will be used.
     *
     * The value can be overridden at various levels, from a global queue-wide setting to values for specific consumers
     * and, finally, the most granular level – individual receive() calls.
     *
     * Values can be overridden in this order, from lowest to highest priority:
     * 1. [QueueOptions.defaultVisibilityTimeout][kolbasa.queue.QueueOptions.defaultVisibilityTimeout]  (lowest priority)
     * 2. [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout]
     * 3. [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout] (highest priority)
     *
     * So, if you set the default timeout to 10 minutes at the queue level, but a specific consumer has a timeout of 5
     * minutes using [ConsumerOptions.visibilityTimeout][kolbasa.consumer.ConsumerOptions.visibilityTimeout], messages received
     * by that consumer (!) will be visible in the queue again after 5 minutes. However, if you set a timeout of 2 minutes for
     * a specific receive() call using [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout],
     * messages received in that call will be available after 2 minutes, overriding the default values for both the consumer
     * and the queue.
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

        val DEFAULT = ReceiveOptions()

        @JvmStatic
        fun builder() = Builder()
    }

}


