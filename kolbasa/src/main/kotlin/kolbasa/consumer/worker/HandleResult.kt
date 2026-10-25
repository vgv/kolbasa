package kolbasa.consumer.worker

/**
 * What a [MessageHandler] decided about one message.
 */
enum class HandleResult {

    /**
     * The message is handled, delete it from the queue.
     */
    DONE,

    /**
     * The message can't be handled right now. Leave it in the queue: it comes back after the visibility timeout.
     * Its attempt is spent anyway, so a message that is skipped every time goes to the DLQ in the end, like a
     * failed one. If the queue has no DLQ, such a message is lost for good once its attempts run out.
     */
    SKIP
}
