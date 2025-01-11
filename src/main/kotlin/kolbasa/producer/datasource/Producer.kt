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
 * If you require a producer working in context of already opened transaction, please, look at [kolbasa.producer.connection.ConnectionAwareProducer].
 *
 * @param Data type of the message
 * @param Meta type of the metadata
 */
interface Producer<Data, Meta : Any> {

    /**
     * Just to send one message without metadata and another options
     *
     * @param data message to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <D, M : Any> send(queue: Queue<D, M>, data: D): SendResult<D, M> {
        return send(queue, SendMessage(data = data))
    }

    /**
     * Send one message with optional metadata and [kolbasa.producer.MessageOptions]
     *
     * @param message data, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <D, M : Any> send(queue: Queue<D, M>, message: SendMessage<D, M>): SendResult<D, M> {
        return send(queue, listOf(message))
    }

    /**
     * Send many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param messages list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <D, M : Any> send(queue: Queue<D, M>, messages: List<SendMessage<D, M>>): SendResult<D, M> {
        return send(queue, SendRequest(data = messages))
    }

    /**
     * Send many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message and
     * custom [kolbasa.producer.SendOptions]
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param request list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <D, M : Any> send(queue: Queue<D, M>, request: SendRequest<D, M>): SendResult<D, M>

    // ----------------------------------------------------------------------------------------------------------

    /**
     * Just to send one message without metadata and another options
     *
     * @param data message to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    @Deprecated("Use send(queue, data) instead", ReplaceWith("send(queue, data)"))
    fun send(data: Data): SendResult<Data, Meta> {
        return send(SendMessage(data = data))
    }

    /**
     * Send one message with optional metadata and [kolbasa.producer.MessageOptions]
     *
     * @param data message, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    @Deprecated("Use send(queue, data) instead", ReplaceWith("send(queue, data)"))
    fun send(data: SendMessage<Data, Meta>): SendResult<Data, Meta> {
        return send(listOf(data))
    }

    /**
     * Send many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param data list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    @Deprecated("Use send(queue, data) instead", ReplaceWith("send(queue, data)"))
    fun send(data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta> {
        return send(SendRequest(data = data))
    }

    /**
     * Send many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message and
     * custom [kolbasa.producer.SendOptions]
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param request list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    @Deprecated("Use send(queue, request) instead", ReplaceWith("send(queue, request)"))
    fun send(request: SendRequest<Data, Meta>): SendResult<Data, Meta>
}
