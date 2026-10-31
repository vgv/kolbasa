package kolbasa.worker

/**
 * What one [BatchProcessor.process] call did.
 */
data class ProcessResult(
    /**
     * How many messages were processed well. They are deleted from the queue.
     */
    val succeeded: Int,

    /**
     * How many messages the [MessageHandler][kolbasa.worker.processor.MessageHandler] skipped for now. They stay in the
     * queue, see [MessageHandler][kolbasa.worker.processor.MessageHandler]. A
     * [QueuePipe][kolbasa.worker.pipe.QueuePipe] never skips a message, so for a pipe it is always 0.
     */
    val skipped: Int,

    /**
     * How many messages failed: the [MessageHandler][kolbasa.worker.processor.MessageHandler] or the transform of a
     * [QueuePipe][kolbasa.worker.pipe.QueuePipe] threw. They stay in the queue.
     */
    val failed: Int,

    /**
     * How many messages were requested, see
     * [QueueProcessorOptions.receiveLimit][kolbasa.worker.processor.QueueProcessorOptions.receiveLimit] and
     * [QueuePipeOptions.receiveLimit][kolbasa.worker.pipe.QueuePipeOptions.receiveLimit].
     */
    val requested: Int
) {

    /**
     * How many messages were received, all three kinds together. Zero means the queue had nothing to give.
     */
    val received: Int = succeeded + skipped + failed

    /**
     * Whether the batch was as large as [requested]. A full batch means the queue probably has more messages right
     * now, so a loop can take the next batch at once. A batch that isn't full means the queue had nothing more to
     * give, and it's time for a pause.
     *
     * Messages that can't be read (see [ReadErrorHandler][kolbasa.consumer.ReadErrorHandler]) are not counted, so a
     * batch with such a message can look not full.
     */
    val isBatchFull: Boolean = received >= requested
}
