package kolbasa.worker

import kolbasa.consumer.Message
import kolbasa.queue.Queue

/**
 * A message that failed: the [MessageHandler][kolbasa.worker.processor.MessageHandler] of a
 * [QueueProcessor][kolbasa.worker.processor.QueueProcessor] or the transform of a
 * [QueuePipe][kolbasa.worker.pipe.QueuePipe] threw. See [ProcessErrorHandler].
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
     * What the [MessageHandler][kolbasa.worker.processor.MessageHandler] or the transform threw.
     */
    val cause: Exception
)
