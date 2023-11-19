package kolbasa.producer

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
 * Kolbasa provides a default, high-performance implementation of [ConnectionAwareProducer]
 * (see class [ConnectionAwareDatabaseProducer]), which uses just plain JDBC and [DataSource][javax.sql.DataSource] and
 * doesn't require any additional dependencies.
 *
 * @param Data type of the message
 * @param Meta type of the metadata
 */
interface ConnectionAwareProducer<Data, Meta : Any> {

    /**
     * Just to send one message without metadata and another options
     *
     * @param connection JDBC connection to use for sending message
     * @param data message to send
     * @returns if success - unique id of the message; if error - throws an exception; if duplicate -
     * [Const.RESERVED_DUPLICATE_ID][kolbasa.schema.Const.RESERVED_DUPLICATE_ID]
     */
    fun send(connection: Connection, data: Data): Long

    /**
     * Send one message with optional metadata and [MessageOptions]
     *
     * @param connection JDBC connection to use for sending message
     * @param data message, metadata (if any) and options (if any) to send
     * @return if success - unique id of the message; if error - throws an exception; if duplicate -
     * [Const.RESERVED_DUPLICATE_ID][kolbasa.schema.Const.RESERVED_DUPLICATE_ID]
     */
    fun send(connection: Connection, data: SendMessage<Data, Meta>): Long

    /**
     * Send many messages with optional metadata and [MessageOptions] defined for every message
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param connection JDBC connection to use for sending messages
     * @param data list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun send(connection: Connection, data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta>

    /**
     * Send many messages with optional metadata and [MessageOptions] defined for every message and custom [SendOptions]
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param connection JDBC connection to use for sending messages
     * @param data list of messages, metadata (if any) and options (if any) to send
     * @param sendOptions options for sending this list of messages, allows to override global [Producer] options
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun send(connection: Connection, data: List<SendMessage<Data, Meta>>, sendOptions: SendOptions): SendResult<Data, Meta>
}
