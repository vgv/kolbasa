package kolbasa.worker.pipe

import kolbasa.producer.datasource.Producer
import kolbasa.queue.Queue

/**
 * Where a [QueuePipe] sends the messages: the target queue and the producer that sends to it.
 */
data class PipeTarget<Data>(
    /**
     * The queue to send the messages to.
     */
    val queue: Queue<Data>,

    /**
     * Sends the messages to [queue].
     */
    val producer: Producer
)
