package kolbasa.worker.pipe

import kolbasa.consumer.Message
import kolbasa.producer.SendMessage

/**
 * Your code that turns one message of the source queue into one message for the target queue, called by a pipe made
 * with [QueuePipe.map].
 *
 * What happens to the message depends on how the transform ends:
 * - returns a message: it is sent to the target queue, and the source message is deleted once its batch is sent;
 * - throws: the message failed. Nothing is sent for it, the message stays in the source queue, and the exception
 *   goes to the [TransformErrorHandler].
 *
 * The metadata of the source message is read only if
 * [ReceiveOptions.readMetadata][kolbasa.consumer.ReceiveOptions.readMetadata] is on in
 * [QueuePipeOptions.receiveOptions]. It is on by default.
 */
fun interface MapTransform<In, Out> {

    /**
     * Turns one message into the message to send.
     *
     * @return the message to send to the target queue
     */
    fun transform(message: Message<In>): SendMessage<Out>
}
