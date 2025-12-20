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
     * Queue visibility timeout. Delay before consumed but not deleted message will be visible to another
     * consumers. By default, value is not set, which means the
     * [QueueOptions.defaultVisibilityTimeout][kolbasa.queue.QueueOptions.defaultVisibilityTimeout] will be used.
     *
     * Can be overridden by [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout]
     * for every consume() call. [ReceiveOptions.visibilityTimeout][kolbasa.consumer.ReceiveOptions.visibilityTimeout]
     * has priority over [visibilityTimeout]
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
