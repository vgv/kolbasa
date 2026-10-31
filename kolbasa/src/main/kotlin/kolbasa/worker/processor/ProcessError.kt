package kolbasa.worker.processor

import kolbasa.consumer.Message
import kolbasa.queue.Queue

/**
 * A message that failed: the [MessageHandler] of a [QueueProcessor] threw. See [ProcessErrorHandler].
 */
data class ProcessError(
    /**
     * The queue the message was received from.
     */
    val queue: Queue<*>,

    /**
     * The message that failed. It stays in the queue.
     */
    val message: Message<*>,

    /**
     * What the [MessageHandler] threw.
     */
    val cause: Exception
)
