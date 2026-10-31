package kolbasa.worker.pipe

import kolbasa.consumer.Message
import kolbasa.producer.SendMessage

/**
 * Your code that turns one message of the source queue into any number of messages for the target queue, called by
 * a pipe made with [QueuePipe.flatMap].
 *
 * What happens to the message depends on how the transform ends:
 * - returns messages: all of them are sent with the batch, and the source message is deleted once the batch is sent;
 * - returns an empty list: nothing is sent for the message, and it is deleted once the batch is sent. This is how to
 *   filter messages out. If the send of the batch fails, a filtered message stays too, and is transformed again with
 *   the whole batch;
 * - throws: the message failed. Nothing is sent for it, the message stays in the source queue, and the exception
 *   goes to the [TransformErrorHandler].
 *
 * The metadata of the source message is read only if
 * [ReceiveOptions.readMetadata][kolbasa.consumer.ReceiveOptions.readMetadata] is on in
 * [QueuePipeOptions.receiveOptions]. It is on by default.
 */
fun interface FlatMapTransform<In, Out> {

    /**
     * Turns one message into the messages to send.
     *
     * @return the messages to send to the target queue, or an empty list to send nothing
     */
    fun transform(message: Message<In>): List<SendMessage<Out>>
}
