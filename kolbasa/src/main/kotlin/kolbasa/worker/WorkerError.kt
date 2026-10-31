package kolbasa.worker

import kolbasa.queue.Queue

/**
 * A batch that a [QueueWorker] failed to process, see [WorkerErrorHandler].
 */
data class WorkerError(
    /**
     * The queue the worker processes.
     */
    val queue: Queue<*>,

    /**
     * What [BatchProcessor.process] threw.
     */
    val cause: Exception
)
