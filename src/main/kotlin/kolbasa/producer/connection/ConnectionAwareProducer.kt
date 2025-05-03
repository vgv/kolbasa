package kolbasa.producer.connection

import kolbasa.producer.SendMessage
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.queue.Queue
import java.sql.Connection

/**
 * Base interface for all producers that must work in context of already opened transaction.
 *
 * Producers are used to send messages to the queues.
 *
 * Sometimes it is useful to send messages in context of already opened and externally managed transaction.
 *
 * For example, you use Hibernate, Exposed, Spring JDBC Template or another framework (even plain JDBC), you may want to
 * send messages and update database in one transaction. When customer creates a new account in your system, you may
 * want to insert new object `Customer(name, email, password_hash)` into `customer` table (via Hibernate, Exposed etc.)
 * and in the same transaction, send a message to the queue `new_registration_events`. If transaction will be rolled back,
 * customer won't be inserted and message will not be sent, otherwise, both operations will be committed.
 *
 * You don't need to handle this manually, this connection aware producer will do it for you. All you need - provide
 * current active JDBC connection to every `send` method.
 *
 * In case of Hibernate, it may look like this
 * ```
 * // Persist customer object
 * session.persist(customer);
 * // Send message to the queue in the same transaction
 * session.doWork(connection -> {
 *     newRegistrationProducer.send(connection, SendMessage(customer))
 * });
 * ```
 * When Hibernate commits transaction (explicitly or, for example, when you use `@Transactional` annotation), message
 * in the queue will be committed too.
 *
 * The same ideas work for consumers or mutators too – you can build completely transactional pipeline just by connecting
 * a few producers/consumers/mutators into one "chain" using the same connection and work inside one active transaction.
 *
 * Kolbasa provides a default, high-performance implementation of [ConnectionAwareProducer]
 * (see [ConnectionAwareDatabaseProducer]), which uses just plain JDBC and doesn't require any additional dependencies.
 */
interface ConnectionAwareProducer {

    /**
     * Sends one message without metadata and another options
     *
     * @param Data type of the message
     * @param Meta type of the metadata
     * @param connection JDBC connection used to send the message
     * @param queue queue to send message
     * @param data message to send
     * @returns [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(connection: Connection, queue: Queue<Data, Meta>, data: Data): SendResult<Data, Meta> {
        return send(connection, queue, SendMessage(data))
    }


    /**
     * Sends one message with optional metadata and [kolbasa.producer.MessageOptions]
     *
     * @param Data type of the message
     * @param Meta type of the metadata
     * @param connection JDBC connection used to send the message
     * @param queue queue to send message
     * @param data message, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(
        connection: Connection,
        queue: Queue<Data, Meta>,
        data: SendMessage<Data, Meta>
    ): SendResult<Data, Meta> {
        return send(connection, queue, listOf(data))
    }

    /**
     * Sends many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param Data type of the message
     * @param Meta type of the metadata
     * @param connection JDBC connection used to send the message
     * @param queue queue to send message
     * @param data list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(
        connection: Connection,
        queue: Queue<Data, Meta>,
        data: List<SendMessage<Data, Meta>>
    ): SendResult<Data, Meta> {
        return send(connection, queue, SendRequest(data = data))
    }

    /**
     * Sends many messages with optional metadata and [kolbasa.producer.MessageOptions] defined for every message
     * and custom [kolbasa.producer.SendOptions]
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param Data type of the message
     * @param Meta type of the metadata
     * @param connection JDBC connection used to send the message
     * @param queue queue to send message
     * @param request list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun <Data, Meta : Any> send(
        connection: Connection,
        queue: Queue<Data, Meta>,
        request: SendRequest<Data, Meta>
    ): SendResult<Data, Meta>
}
