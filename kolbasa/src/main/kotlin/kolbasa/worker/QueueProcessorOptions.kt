package kolbasa.worker

import kolbasa.consumer.ReceiveOptions
import kolbasa.queue.Checks
import kolbasa.telemetry.tracing.Tracing

/**
 * Options of a [QueueProcessor].
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = QueueProcessorOptions(receiveLimit = 50)
 *
 * val processor = QueueProcessor(consumer, queue, options) { message ->
 *     process(message.data)
 *     HandleResult.DONE
 * }
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var options = QueueProcessorOptions.builder()
 *     .receiveLimit(50)
 *     .build();
 *
 * var processor = new QueueProcessor<>(consumer, queue, options, message -> {
 *     process(message.getData());
 *     return HandleResult.DONE;
 * });
 * ```
 */
data class QueueProcessorOptions(
    /**
     * How many messages one [QueueProcessor.process] call receives at most. The default is 10.
     *
     * The messages of one batch are handled one after another and deleted together at the end. So the whole batch
     * must be handled within the visibility timeout. Otherwise, its first messages come back to the queue before
     * they are deleted, and are handled once more.
     */
    val receiveLimit: Int = DEFAULT_RECEIVE_LIMIT,

    /**
     * Options of every `receive()` call: filter, order, visibility timeout and so on. See [ReceiveOptions].
     */
    val receiveOptions: ReceiveOptions = ReceiveOptions.DEFAULT,

    /**
     * What to do when the [MessageHandler] throws. By default, the error goes to the log, see
     * [ProcessErrorHandler.SYSTEM_LOG].
     */
    val errorHandler: ProcessErrorHandler = ProcessErrorHandler.SYSTEM_LOG,

    /**
     * Tracing of the handler calls, see [Tracing.process]. By default, the value is unspecified (null), meaning that
     * global [Kolbasa.tracing][kolbasa.Kolbasa.tracing] will be used.
     *
     * The processor can't see the tracing of its consumer. So if you set
     * [ConsumerOptions.tracing][kolbasa.consumer.ConsumerOptions.tracing] for the consumer, set the same here, or the
     * receive and the handler calls go to different hooks.
     */
    val tracing: Tracing? = null
) {

    init {
        Checks.checkReceiveLimit(receiveLimit)
    }

    /**
     * Builder for flexible [QueueProcessorOptions] creation, when only some of the properties need to be set.
     */
    class Builder {
        private var receiveLimit: Int = DEFAULT_RECEIVE_LIMIT
        private var receiveOptions: ReceiveOptions = ReceiveOptions.DEFAULT
        private var errorHandler: ProcessErrorHandler = ProcessErrorHandler.SYSTEM_LOG
        private var tracing: Tracing? = null

        /**
         * Sets [QueueProcessorOptions.receiveLimit] – how many messages one `process()` call receives at most.
         */
        fun receiveLimit(receiveLimit: Int) = apply { this.receiveLimit = receiveLimit }

        /**
         * Sets [QueueProcessorOptions.receiveOptions] – the options of every `receive()` call.
         */
        fun receiveOptions(receiveOptions: ReceiveOptions) = apply { this.receiveOptions = receiveOptions }

        /**
         * Sets [QueueProcessorOptions.errorHandler] – what to do when the message handler throws.
         */
        fun errorHandler(errorHandler: ProcessErrorHandler) = apply { this.errorHandler = errorHandler }

        /**
         * Sets [QueueProcessorOptions.tracing] – the tracing of the handler calls, used instead of the global one.
         */
        fun tracing(tracing: Tracing) = apply { this.tracing = tracing }

        /**
         * Creates a new [QueueProcessorOptions] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = QueueProcessorOptions(receiveLimit, receiveOptions, errorHandler, tracing)
    }

    companion object {

        /**
         * How many messages one `process()` call receives at most, unless [receiveLimit] says otherwise.
         */
        const val DEFAULT_RECEIVE_LIMIT = 10

        /**
         * Default options: up to 10 messages per call, default receive options, errors go to the log.
         */
        @JvmField
        val DEFAULT = QueueProcessorOptions()

        @JvmStatic
        fun builder() = Builder()
    }
}
