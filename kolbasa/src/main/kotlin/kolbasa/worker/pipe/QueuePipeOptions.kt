package kolbasa.worker.pipe

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
     * How many messages one [QueuePipe.process] call receives at most. The default is 10, and 100 in [AS_IS_DEFAULT],
     * the default options of [QueuePipe.asIs].
     *
     * The batch must be transformed, sent and deleted within the visibility timeout. Otherwise, its messages come back
     * to the source queue and are sent once more. A batch takes about `receiveLimit × time of the transform`, plus one
     * round trip to the target queue for the send.
     *
     * [QueuePipe.asIs] does almost nothing with a message, so its batch costs one round trip, however many messages it
     * has. That is why its default is larger. Your transform may be slow, for example if it calls another service, so
     * for it the default is as careful as in [QueueProcessorOptions][kolbasa.worker.processor.QueueProcessorOptions].
     * If your transform is fast, raise the limit: the pipe moves more messages per round trip.
     *
     * To take larger batches, raise the visibility timeout too. A rule of thumb: the visibility timeout should be at
     * least `(receiveLimit × average time of the transform + time of the send) × 2`.
     *
     * On a local network, the send takes milliseconds, and you can leave it out. To another data center or another
     * continent, or on a slow link, it can take several seconds: it is one call, but it carries the whole batch. For
     * example, 100 messages with a transform of 50 ms each and a send of 3 seconds need at least
     * `(100 × 0.05 s + 3 s) × 2 = 16 s`.
     *
     * **Watch this closely.** A slow transform adds up fast. If it calls another service for half a second, the same
     * batch takes `100 × 0.5 s + 3 s = 53 s`, dangerously close to the default visibility timeout of 60 seconds. A few
     * slow calls, and the batch doesn't fit: its messages come back to the source queue and are sent twice. Nothing
     * fails and nothing is logged, you just get duplicates in the target queue. That is why the rule doubles the time.
     * See [ReceiveOptions.visibilityTimeout] for where to set it.
     */
    val receiveLimit: Int = DEFAULT_RECEIVE_LIMIT,

    /**
     * Options of every `receive()` call: filter, order, visibility timeout and so on. See [ReceiveOptions].
     *
     * By default, the metadata is read, so that a transform can use it. If you set your own options and the transform
     * needs the metadata, keep [ReceiveOptions.readMetadata] on. [QueuePipe.asIs] always reads it.
     */
    val receiveOptions: ReceiveOptions = DEFAULT_RECEIVE_OPTIONS,

    /**
     * What to do when the transform throws. By default, the error goes to the log, see
     * [TransformErrorHandler.SYSTEM_LOG].
     */
    val errorHandler: TransformErrorHandler = TransformErrorHandler.SYSTEM_LOG
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
        private var errorHandler: TransformErrorHandler = TransformErrorHandler.SYSTEM_LOG

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
        fun errorHandler(errorHandler: TransformErrorHandler) = apply { this.errorHandler = errorHandler }

        /**
         * Creates a new [QueuePipeOptions] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = QueuePipeOptions(receiveLimit, receiveOptions, errorHandler)
    }

    companion object {

        /**
         * How many messages one `process()` call receives at most, unless [receiveLimit] says otherwise.
         */
        const val DEFAULT_RECEIVE_LIMIT = 10

        /**
         * The receive limit in [AS_IS_DEFAULT].
         */
        const val AS_IS_RECEIVE_LIMIT = 100

        /**
         * The receive options of a pipe, unless [receiveOptions] says otherwise: the defaults, but with the metadata.
         */
        @JvmField
        val DEFAULT_RECEIVE_OPTIONS = ReceiveOptions(readMetadata = true)

        /**
         * Default options: up to 10 messages per call, the metadata is read, errors go to the log.
         */
        @JvmField
        val DEFAULT = QueuePipeOptions()

        /**
         * Default options of [QueuePipe.asIs]: the same as [DEFAULT], but up to 100 messages per call.
         */
        @JvmField
        val AS_IS_DEFAULT = QueuePipeOptions(receiveLimit = AS_IS_RECEIVE_LIMIT)

        @JvmStatic
        fun builder() = Builder()
    }
}
