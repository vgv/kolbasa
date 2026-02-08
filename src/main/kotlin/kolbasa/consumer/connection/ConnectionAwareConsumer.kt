package kolbasa.consumer.connection

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.sql.Connection

/**
 * Base interface for all consumers that must work in context of already opened transaction.
 *
 * Consumers are used to receive messages from the queues.
 *
 * Sometimes it is useful to receive/process messages in context of already opened and externally managed transaction.
 *
 * For example, if you are using Hibernate, Exposed, Spring JDBC Template or another framework (even plain JDBC), you may
 * want to receive messages and update the database in a single transaction. For example, when you receive a message from
 * the `new_sale_registered` queue, you may want to decrease the stock amount for purchased items in your catalog. To do this,
 * you need to insert/update multiple rows in your `catalog` table, but you should only decrease the stock once, otherwise
 * you will get incorrect results. So, you need to process a message from a queue and update multiple rows in your database
 * atomically, as a single operation - either both at once or neither.
 *
 * If the transaction will be rolled back, the `new_sale` message will not be deleted from the `new_sale_registered` queue
 * and your `catalog` table won't be updated, allowing you to retry the entire operation later. Otherwise, both operations
 * will be committed.
 *
 * You don't need to handle this manually, this connection aware consumer will do it for you. All you need - provide
 * current active JDBC connection to every `receive` method.
 *
 * In case of Hibernate, it may look like this
 * ```kotlin
 * session.doWork { connection ->
 *    consumer.receive(queue)?.let { message ->
 *       // business logic
 *       session.persist(businessEntity)
 *       // delete processed message inside the same transaction
 *       consumer.delete(connection, queue, message)
 *    }
 * }
 * ```
 * When Hibernate commits transaction (explicitly or, for example, when you use `@Transactional` annotation), your business
 * entity changes and message removal will be commited at the same time.
 *
 * The same ideas work for producers or mutators too – you can build completely transactional pipeline just by connecting
 * a few producers/consumers/mutators into one "chain" using the same connection and work inside one active transaction.
 *
 * Kolbasa provides a default, high-performance implementation of [ConnectionAwareConsumer]
 * (see [ConnectionAwareDatabaseConsumer]), which uses just plain JDBC and doesn't require any additional dependencies.
 */
interface ConnectionAwareConsumer {

    /**
     * Receives one message from the queue, if any. Returns null if the queue is empty.
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which to receive a message
     * @return message or null if the queue is empty
     */
    fun <Data> receive(connection: Connection, queue: Queue<Data>): Message<Data>? {
        return receive(connection, queue, ReceiveOptions.DEFAULT)
    }

    /**
     * Receives one message from the queue using custom filters, if any. Returns null if no message matches the filters.
     *
     * Filters can be specified using nice Kotlin lambda syntax as follows:
     * ```kotlin
     * // Try to read a message with (userId<=10 OR userId=78) from the queue
     * val message = consumer.receive(queue) {
     *     // Type-safe DSL to filter messages
     *     (Metadata::userId lessEq 10) or (Metadata::userId eq 78)
     * }
     * ```
     *
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which to receive a message
     * @param filter custom, user-defined filters to receive only specific messages from the queue
     * @return message or null if no message matches the filters
     */
    fun <Data> receive(
        connection: Connection,
        queue: Queue<Data>,
        filter: Filter.() -> Condition
    ): Message<Data>? {
        return receive(connection, queue, ReceiveOptions(filter = filter(Filter)))
    }

    /**
     * Receives one message from the queue using custom [options][ReceiveOptions], if any. Returns null if no message
     * matches the filters specified in the `receiveOptions` method parameter.
     *
     * Custom [ReceiveOptions] allows to specify custom filters, messages ordering, visibility timeout and so on. For
     * more details please read [ReceiveOptions] docs.
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which to receive a message
     * @param receiveOptions custom options (filters, ordering etc.)
     * @return message or null if no message matches the filters specified in the `receiveOptions`
     */
    fun <Data> receive(
        connection: Connection,
        queue: Queue<Data>,
        receiveOptions: ReceiveOptions
    ): Message<Data>? {
        val result = receive(connection, queue, limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    /**
     * Receives up to `limit` messages from the queue, if any. Returns empty list if the queue is empty.
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which to receive a message
     * @param limit number of messages to receive
     * @return messages or an empty list if the queue is empty
     */
    fun <Data> receive(connection: Connection, queue: Queue<Data>, limit: Int): List<Message<Data>> {
        return receive(connection, queue, limit, ReceiveOptions.DEFAULT)
    }

    /**
     * Receives up to `limit` messages from the queue using custom filters, if any. Returns an empty list if no message
     * matches the filters.
     *
     * Filters can be specified using nice Kotlin lambda syntax as follows:
     * ```kotlin
     * // Try to read up to 100 messages with (userId<=10 OR userId=78) from the queue
     * val messages = consumer.receive(queue, 100) {
     *     // Type-safe DSL to filter messages
     *     (Metadata::userId lessEq 10) or (Metadata::userId eq 78)
     * }
     * ```
     *
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which to receive a message
     * @param limit number of messages to receive
     * @param filter custom, user-defined filters to receive only specific messages from the queue
     * @return messages or an empty list if no message matches the filters
     */
    fun <Data> receive(
        connection: Connection,
        queue: Queue<Data>,
        limit: Int,
        filter: Filter.() -> Condition
    ): List<Message<Data>> {
        return receive(connection, queue, limit, ReceiveOptions(filter = filter(Filter)))
    }

    /**
     * Receives up to `limit` messages from the queue using custom [options][ReceiveOptions], if any. Returns an empty list
     * if no message matches the filters specified in the `receiveOptions` method parameter.
     *
     * Custom [ReceiveOptions] allows to specify custom filters, messages ordering, visibility timeout and so on. For
     * more details please read [ReceiveOptions] docs.
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which to receive a message
     * @param limit number of messages to receive
     * @param receiveOptions custom options (filters, ordering etc.)
     * @return messages or an empty list if no message matches the filters specified in the `receiveOptions`
     */
    fun <Data> receive(
        connection: Connection,
        queue: Queue<Data>,
        limit: Int,
        receiveOptions: ReceiveOptions
    ): List<Message<Data>>

    // Delete

    /**
     * Deletes the processed message from the queue.
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which the message should be deleted
     * @param message message to delete
     */
    fun <Data> delete(connection: Connection, queue: Queue<Data>, message: Message<Data>): Int {
        return delete(connection, queue, message.id)
    }

    /**
     * Deletes the processed messages from the queue.
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which the messages should be deleted
     * @param messages messages to delete
     */
    fun <Data> delete(
        connection: Connection,
        queue: Queue<Data>,
        messages: Collection<Message<Data>>
    ): Int {
        return delete(connection, queue, messages.map(Message<Data>::id))
    }

    /**
     * Deletes the processed message from the queue by message [identifier][kolbasa.producer.Id]
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which the message should be deleted
     * @param messageId identifier of the message to delete
     */
    fun <Data> delete(connection: Connection, queue: Queue<Data>, messageId: Id): Int {
        return delete(connection, queue, listOf(messageId))
    }

    /**
     * Deletes the processed messages from the queue by messages [identifiers][kolbasa.producer.Id]
     *
     * @param connection JDBC connection used to receive the message
     * @param queue queue from which the messages should be deleted
     * @param messageIds identifiers of the messages to delete
     */
    fun <Data> delete(connection: Connection, queue: Queue<Data>, messageIds: List<Id>): Int
}
