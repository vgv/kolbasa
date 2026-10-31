package kolbasa.worker

import kolbasa.consumer.Message

/**
 * Your business code for one message, called by [QueueProcessor].
 *
 * What happens to the message depends on how the handler ends:
 * - returns [HandleResult.DONE]: the message is handled, and it is deleted from the queue;
 * - returns [HandleResult.SKIP]: the message can't be handled right now. It stays in the queue and comes back after
 *   the visibility timeout;
 * - throws: the message failed. It stays in the queue, as a skipped one, and the exception goes to the
 *   [ProcessErrorHandler].
 *
 * To send a message to the DLQ at once, or to retry it after a delay of your choice, change it with a
 * [Mutator][kolbasa.mutator.datasource.Mutator] and return [HandleResult.SKIP]. For example, the mutations
 * [SetRemainingAttempts(0)][kolbasa.mutator.SetRemainingAttempts] and
 * [SetScheduledAt(Duration.ZERO)][kolbasa.mutator.SetScheduledAt] make the message dead, and the next sweep moves it
 * to the DLQ.
 */
fun interface MessageHandler<Data> {

    /**
     * Handles one message.
     *
     * @return [HandleResult.DONE] if the message is handled and can be deleted, [HandleResult.SKIP] to leave it
     * for now
     */
    fun handle(message: Message<Data>): HandleResult
}
