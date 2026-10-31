package kolbasa.worker.pipe

import kolbasa.consumer.Message
import kolbasa.queue.Queue

/**
 * A message that failed: the transform of a [QueuePipe] threw. See [TransformErrorHandler].
 */
data class TransformError(
    /**
     * The queue the message was received from, the source queue of the pipe.
     */
    val queue: Queue<*>,

    /**
     * The message that failed. It stays in the source queue.
     */
    val message: Message<*>,

    /**
     * What the transform threw. When a [BatchTransform] throws, every message of the batch comes with the same
     * exception.
     */
    val cause: Exception
)
