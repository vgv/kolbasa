package kolbasa.consumer

import kolbasa.queue.Checks
import java.time.Duration

data class ConsumerOptions(
    /**
     * Arbitrary consumer name. Every message, consumed by this consumer will have this name in the 'consumer' column of
     * corresponding queue table.
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
    val visibilityTimeout: Duration? = null
) {

    init {
        Checks.checkConsumerName(consumer)
        Checks.checkVisibilityTimeout(visibilityTimeout)
    }

    class Builder {
        private var consumer: String? = null
        private var visibilityTimeout: Duration? = null

        fun consumer(consumer: String) = apply { this.consumer = consumer }
        fun visibilityTimeout(visibilityTimeout: Duration) = apply { this.visibilityTimeout = visibilityTimeout }

        fun build() = ConsumerOptions(consumer, visibilityTimeout)
    }

    companion object {

        val DEFAULT = ConsumerOptions()

        @JvmStatic
        fun builder() = Builder()
    }

}
