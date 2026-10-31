package kolbasa.worker

import kolbasa.queue.Queue

/**
 * Processes one batch from a queue per [process] call. A [QueueWorker] calls it in a loop.
 *
 * The usual one is [QueueProcessor][kolbasa.worker.processor.QueueProcessor]: it passes every message to your
 * [MessageHandler][kolbasa.worker.processor.MessageHandler].
 */
interface BatchProcessor {

    /**
     * The queue the batches come from. The worker puts its name into the thread names and passes it in [WorkerError].
     */
    val queue: Queue<*>

    /**
     * Processes one batch.
     *
     * @return what the batch did. A worker takes the next batch at once if [ProcessResult.isBatchFull], and pauses
     * otherwise
     */
    fun process(): ProcessResult
}
