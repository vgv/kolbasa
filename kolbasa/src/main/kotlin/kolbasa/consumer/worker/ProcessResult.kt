package kolbasa.consumer.worker

/**
 * What one [QueueProcessor.process] call did.
 */
data class ProcessResult(
    /**
     * How many messages the [MessageHandler] handled. They are deleted from the queue.
     */
    val succeeded: Int,

    /**
     * How many messages the [MessageHandler] skipped for now. They stay in the queue, see [MessageHandler].
     */
    val skipped: Int,

    /**
     * How many messages the [MessageHandler] failed to handle. They stay in the queue, see [MessageHandler].
     */
    val failed: Int
) {

    /**
     * How many messages were received, all three kinds together. Zero means the queue had nothing to give.
     */
    val received: Int = succeeded + skipped + failed
}
