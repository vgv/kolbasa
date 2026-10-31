package kolbasa.worker

import kolbasa.consumer.Message
import kolbasa.consumer.datasource.Consumer
import kolbasa.producer.Id
import kolbasa.producer.PartialInsert
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.datasource.Producer
import kolbasa.queue.Queue

/**
 * What a transform made of one batch.
 */
internal class TransformedBatch<Out>(
    /**
     * The messages to send.
     */
    val toSend: List<SendMessage<Out>>,

    /**
     * The source messages to delete once [toSend] is sent.
     */
    val toDelete: List<Id>,

    /**
     * How many source messages the transform failed on. They stay in the source queue.
     */
    val failed: Int,

    /**
     * The options of the send. The pipe always sends all or nothing, so their partial insert is replaced.
     */
    val sendOptions: SendOptions = SendOptions.DEFAULT
)

/**
 * The work of a [QueuePipe], with the real types of its queues: receives a batch, transforms it, sends all of it or
 * nothing, and deletes the source messages.
 */
internal class PipeEngine<In, Out>(
    private val from: Queue<In>,
    private val consumer: Consumer,
    private val to: Queue<Out>,
    private val producer: Producer,
    private val options: QueuePipeOptions,
    private val transform: (List<Message<In>>) -> TransformedBatch<Out>
) {

    // The result of every empty batch, made once to avoid extra allocations
    private val emptyResult = ProcessResult(succeeded = 0, skipped = 0, failed = 0, requested = options.receiveLimit)

    fun process(): ProcessResult {
        val messages = consumer.receive(from, options.receiveLimit, options.receiveOptions)
        if (messages.isEmpty()) {
            return emptyResult
        }

        val batch = transform(messages)

        // All or nothing: if the send fails, nothing is deleted, and the whole batch comes back to be transformed
        // again. Only the partial insert is replaced, the other options are the transform's, or else the producer's
        if (batch.toSend.isNotEmpty()) {
            val sendOptions = batch.sendOptions.copy(partialInsert = PartialInsert.PROHIBITED)
            producer.send(to, SendRequest(batch.toSend, sendOptions)).throwExceptionIfAny()
        }

        if (batch.toDelete.isNotEmpty()) {
            consumer.delete(from, batch.toDelete)
        }

        return ProcessResult(
            succeeded = batch.toDelete.size,
            skipped = 0,
            failed = batch.failed,
            requested = options.receiveLimit
        )
    }
}
