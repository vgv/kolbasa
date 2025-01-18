package kolbasa.producer.datasource

import kolbasa.producer.SendMessage
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.queue.Queue

/**
 * Base interface for all producers
 *
 * Producers are used to send messages to the queues.
 *
 * This is a basic interface, implementations of which may behave differently. Kolbasa provides a default, high-performance
 * implementation of [Producer], which uses just plain JDBC and [DataSource][javax.sql.DataSource] and doesn't require
 * any additional dependencies.
 * This default, provided implementation completely hides database handling and transaction management from the user.
 *
 * If you require a producer working in context of already opened transaction,
 * please, look at [ConnectionAwareProducer][kolbasa.producer.connection.ConnectionAwareProducer].
 */
interface Producer {

    /**
     * Just to send one message without metadata and another options
     *
     * @param queue queue to send the message to
     * @param data message to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(queue: Queue<Data, Meta>, data: Data): SendResult<Data, Meta> {
        return send(queue, SendMessage(data = data))
    }

    /**
     * Send one message with optional metadata and [kolbasa.producer.MessageOptions]
     *
     * @param queue queue to send the message to
     * @param message data, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(queue: Queue<Data, Meta>, message: SendMessage<Data, Meta>): SendResult<Data, Meta> {
        return send(queue, listOf(message))
    }

    /**
     * Send many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param queue queue to send the message to
     * @param messages list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(queue: Queue<Data, Meta>, messages: List<SendMessage<Data, Meta>>): SendResult<Data, Meta> {
        return send(queue, SendRequest(data = messages))
    }

    /**
     * Send many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message and
     * custom [kolbasa.producer.SendOptions]
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param queue queue to send the message to
     * @param request list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(queue: Queue<Data, Meta>, request: SendRequest<Data, Meta>): SendResult<Data, Meta>

}
