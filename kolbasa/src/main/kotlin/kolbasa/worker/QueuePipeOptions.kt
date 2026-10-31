package kolbasa.worker

import kolbasa.consumer.ReceiveOptions
import kolbasa.queue.Checks

/**
 * Options of a [QueuePipe].
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = QueuePipeOptions(receiveLimit = 500)
 *
 * val pipe = QueuePipe.map(from, consumer, to, producer, options) { message ->
 *     SendMessage(message.data)
 * }
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var options = QueuePipeOptions.builder()
 *     .receiveLimit(500)
 *     .build();
 *
 * var pipe = QueuePipe.map(from, consumer, to, producer, options, message -> {
 *     return new SendMessage<>(message.getData());
 * });
 * ```
 */
data class QueuePipeOptions(
    /**
     * How many messages one [QueuePipe.process] call receives at most. The default is 100.
     *
     * It is larger than in [QueueProcessorOptions], because the pipe sends the whole batch in one call. So a batch
     * costs one round trip to the target queue, however many messages it has. The batch must still be sent and
     * deleted within the visibility timeout. Otherwise, its messages come back to the source queue and are sent once
     * more.
     */
    val receiveLimit: Int = DEFAULT_RECEIVE_LIMIT,

    /**
     * Options of every `receive()` call: filter, order, visibility timeout and so on. See [ReceiveOptions].
     *
     * By default, the metadata is read, so that [QueuePipe.asIs] moves it and a transform can use it. If you
     * set your own options, keep [ReceiveOptions.readMetadata] on for that.
     */
    val receiveOptions: ReceiveOptions = DEFAULT_RECEIVE_OPTIONS,

    /**
     * What to do when the transform throws. By default, the error goes to the log, see
     * [ProcessErrorHandler.SYSTEM_LOG].
     */
    val errorHandler: ProcessErrorHandler = ProcessErrorHandler.SYSTEM_LOG
) {

    init {
        Checks.checkReceiveLimit(receiveLimit)
    }

    /**
     * Builder for flexible [QueuePipeOptions] creation, when only some of the properties need to be set.
     */
    class Builder {
        private var receiveLimit: Int = DEFAULT_RECEIVE_LIMIT
        private var receiveOptions: ReceiveOptions = DEFAULT_RECEIVE_OPTIONS
        private var errorHandler: ProcessErrorHandler = ProcessErrorHandler.SYSTEM_LOG

        /**
         * Sets [QueuePipeOptions.receiveLimit] – how many messages one `process()` call receives at most.
         */
        fun receiveLimit(receiveLimit: Int) = apply { this.receiveLimit = receiveLimit }

        /**
         * Sets [QueuePipeOptions.receiveOptions] – the options of every `receive()` call.
         */
        fun receiveOptions(receiveOptions: ReceiveOptions) = apply { this.receiveOptions = receiveOptions }

        /**
         * Sets [QueuePipeOptions.errorHandler] – what to do when the transform throws.
         */
        fun errorHandler(errorHandler: ProcessErrorHandler) = apply { this.errorHandler = errorHandler }

        /**
         * Creates a new [QueuePipeOptions] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = QueuePipeOptions(receiveLimit, receiveOptions, errorHandler)
    }

    companion object {

        /**
         * How many messages one `process()` call receives at most, unless [receiveLimit] says otherwise.
         */
        const val DEFAULT_RECEIVE_LIMIT = 100

        /**
         * The receive options of a pipe, unless [receiveOptions] says otherwise: the defaults, but with the metadata.
         */
        @JvmField
        val DEFAULT_RECEIVE_OPTIONS = ReceiveOptions(readMetadata = true)

        /**
         * Default options: up to 100 messages per call, the metadata is read, errors go to the log.
         */
        @JvmField
        val DEFAULT = QueuePipeOptions()

        @JvmStatic
        fun builder() = Builder()
    }
}
