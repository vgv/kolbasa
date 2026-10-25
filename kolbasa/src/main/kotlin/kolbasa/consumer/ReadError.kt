package kolbasa.consumer

import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import java.time.Instant

/**
 * A received message that can't be read, see [ReadErrorHandler].
 *
 * It has the same fields as [Message], except the data and the meta fields, which are what can't be read.
 */
data class ReadError(
    /**
     * The queue the message was received from.
     */
    val queue: Queue<*>,

    /**
     * The id of the message, see [Message.id].
     */
    val messageId: Id,

    /**
     * The node the message was received from. Outside a cluster, its id is empty.
     */
    val nodeId: NodeId,

    /**
     * When the message was created, see [Message.createdAt].
     */
    val createdAt: Instant,

    /**
     * When the message was taken for processing, see [Message.processingAt].
     */
    val processingAt: Instant,

    /**
     * When the message can be received again, see [Message.scheduledAt].
     */
    val scheduledAt: Instant,

    /**
     * How many attempts are left, see [Message.remainingAttempts]. If it is 0, the message won't be received again:
     * it goes to the DLQ, or is deleted if the queue has no DLQ.
     */
    val remainingAttempts: Int,

    /**
     * Why the message can't be read.
     */
    val cause: Exception
) {

    /**
     * Whether this was the last attempt, see [Message.isLastAttempt].
     */
    fun isLastAttempt(): Boolean = remainingAttempts == 0
}
