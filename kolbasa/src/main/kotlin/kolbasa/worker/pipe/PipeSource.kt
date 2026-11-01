package kolbasa.worker.pipe

import kolbasa.consumer.datasource.Consumer
import kolbasa.queue.Queue

/**
 * Where a [QueuePipe] takes the messages from: the source queue and the consumer that receives from it.
 */
data class PipeSource<Data>(
    /**
     * The queue to take the messages from.
     */
    val queue: Queue<Data>,

    /**
     * Receives the messages from [queue].
     */
    val consumer: Consumer
)
