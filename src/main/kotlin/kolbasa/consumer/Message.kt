package kolbasa.consumer

import kolbasa.producer.Id
import kolbasa.queue.meta.MetaValues

data class Message<Data>(
    /**
     * Unique message identifier
     *
     * The identifier is always unique, regardless of whether you're using a Kolbasa cluster or standalone mode.
     *
     * If you use a Kolbasa cluster, this identifier is unique within the cluster. There can be up to 1024 servers in a cluster,
     * but in any case, each message will have a unique ID.
     *
     * If you do not use a Kolbasa cluster, that is, you work in standalone mode, then the message is unique only within one
     * database server, but there are no problems – you only have one server.
     */
    val id: Id,

    /**
     * Timestamp when this message was created
     */
    val createdAt: Long,

    /**
     * Timestamp when this message was taken for processing
     */
    val processingAt: Long,

    /**
     * Timestamp when this message will be available to receive again, e.g. visibility timeout
     */
    val scheduledAt: Long,

    /**
     * How many attempts we have to process this message
     *
     * Every time a message is received, this field is decremented by 1. If all attempts to process the message are
     * unsuccessful, the number of attempts will eventually become 0, and the message will be lost; it will be impossible
     * to receive it again.
     *
     * A number of attempts of 0 means this is the last time this message has been received from the queue; you will not
     * receive it again. This can be useful for additional actions, such as logging, sending a different message to a different
     * queue, writing to a database, and so on.
     *
     * To avoid having to remember and work with magic constants (0), there is a convenient [isLastAttempt] method for checking
     *
     * ```kotlin
     * fun processOneMessage(message: Message<Data, Meta>) {
     *   try {
     *     // ... business code to process the message ...
     *   } catch (e: Exception) {
     *     if (message.isLastAttempt()) {
     *       // This was the last attempt to process this message, but it failed again. Let's log it
     *       log.error("Can't process: $message")
     *     }
     *   }
     * }
     * ```
     *
     */
    val remainingAttempts: Int,

    /**
     * Message data
     */
    val data: Data,

    /**
     * Metadata, if
     * 1) There is metadata attached to the message
     * 2) [ReceiveOptions.readMetadata][kolbasa.consumer.ReceiveOptions.readMetadata] set to true
     */
    val meta: MetaValues = MetaValues.EMPTY
) {

    internal var openTelemetryData: Map<String, String>? = null

    fun isLastAttempt(): Boolean = (remainingAttempts == 0)

}
