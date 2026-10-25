package kolbasa.worker

import kolbasa.consumer.Message
import kolbasa.queue.Queue

/**
 * A message that the [MessageHandler] failed to handle, see [ProcessErrorHandler].
 */
data class ProcessError(
    /**
     * The queue the message was received from.
     */
    val queue: Queue<*>,

    /**
     * The message that failed. It stays in the queue, see [MessageHandler].
     */
    val message: Message<*>,

    /**
     * What the [MessageHandler] threw.
     */
    val cause: Exception
)
