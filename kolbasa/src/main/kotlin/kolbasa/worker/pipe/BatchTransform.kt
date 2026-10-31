package kolbasa.worker.pipe

import kolbasa.consumer.Message
import kolbasa.producer.SendRequest
import kolbasa.worker.ProcessErrorHandler

/**
 * Your code that turns a whole batch of the source queue into the messages for the target queue, called by a pipe
 * made with [QueuePipe.mapBatch].
 *
 * Use it when the batch as a whole matters: to choose the shard or other send options of every batch, to ask another
 * system once for the whole batch, or to merge several messages into one.
 *
 * The batch is one unit. What happens to it depends on how the transform ends:
 * - returns a request: its messages are sent with its options, and all the source messages are deleted once they are
 *   sent. The pipe always sends all or nothing, so the partial insert of the request options is replaced with
 *   [PartialInsert.PROHIBITED][kolbasa.producer.PartialInsert.PROHIBITED];
 * - returns a request without messages: nothing is sent, and all the source messages are deleted;
 * - throws: the whole batch failed. Nothing is sent, all the messages stay in the source queue, and every one of them
 *   goes to the [ProcessErrorHandler] with the same exception.
 *
 * So one message that the transform can't handle fails its whole batch, every time, and the other messages of the
 * batch spend their attempts with it. Deal with such messages inside the transform.
 *
 * The metadata of the source messages is read only if
 * [ReceiveOptions.readMetadata][kolbasa.consumer.ReceiveOptions.readMetadata] is on in
 * [QueuePipeOptions.receiveOptions]. It is on by default.
 */
fun interface BatchTransform<In, Out> {

    /**
     * Turns one batch into the messages to send, and the options to send them with.
     *
     * @return the messages to send to the target queue and the send options, or a request without messages to send
     * nothing
     */
    fun transform(batch: List<Message<In>>): SendRequest<Out>
}
