package kolbasa.consumer.datasource

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.producer.Id
import kolbasa.queue.Queue

/**
 * Base interface for all consumers
 *
 * Consumers are used to receive messages from the queue and further process them. The consumer does not know and in no
 * way controls the sending of messages to the queue by the producers. They do not know which producer and when put
 * the messages in the queue and do not depend on the number of producers at the moment – there may be one, ten, or none,
 * good design implies that the work of the consumer does not depend on this in any way. Their only task is to receive a
 * message from the queue and give it to the business code for processing.
 *
 * This is a basic interface, implementations of which may behave differently.
 *
 * Kolbasa provides a default, high-performance implementation [DatabaseConsumer], which uses just plain JDBC and
 * [DataSource][javax.sql.DataSource] and doesn't require any additional dependencies. This default, provided implementation
 * completely hides database handling and transaction management from the user.
 *
 * If you require a consumer working in context of already opened transaction,
 * please look at [ConnectionAwareConsumer][kolbasa.consumer.connection.ConnectionAwareConsumer].
 */
interface Consumer {

    /**
     * Receives one message from the queue, if any. Returns null if the queue is empty.
     *
     * @param queue queue from which to receive a message
     * @return message or null if the queue is empty
     */
    fun <Data, Meta : Any> receive(queue: Queue<Data, Meta>): Message<Data, Meta>? {
        return receive(queue, ReceiveOptions())
    }

    /**
     * Receives one message from the queue using custom filters, if any. Returns null if no message matches the filters.
     *
     * Filters can be specified using nice Kotlin lambda syntax as follows:
     * ```
     * // Try to read a message with (userId<=10 OR userId=78) from the queue
     * val message = consumer.receive(queue) {
     *     // Type-safe DSL to filter messages
     *     (Metadata::userId lessEq 10) or (Metadata::userId eq 78)
     * }
     * ```
     *
     * If you use Java, please take a look at [JavaField][kolbasa.consumer.JavaField] class for examples.
     *
     * @param queue queue from which to receive a message
     * @param filter custom, user-defined filters to receive only specific messages from the queue
     * @return message or null if no message matches the filters
     */
    fun <Data, Meta : Any> receive(queue: Queue<Data, Meta>, filter: Filter.() -> Condition<Meta>): Message<Data, Meta>? {
        return receive(queue, ReceiveOptions(filter = filter(Filter)))
    }

    /**
     * Receives one message from the queue using custom [options][ReceiveOptions], if any. Returns null if no message
     * matches the filters specified in the `receiveOptions` method parameter.
     *
     * Custom [ReceiveOptions] allows to specify custom filters, messages ordering, visibility timeout and so on. For
     * more details please read [ReceiveOptions] docs.
     *
     * @param queue queue from which to receive a message
     * @param receiveOptions custom options (filters, ordering etc.)
     * @return message or null if no message matches the filters specified in the `receiveOptions`
     */
    fun <Data, Meta : Any> receive(queue: Queue<Data, Meta>, receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>? {
        val result = receive(queue, limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    /**
     * Receives up to `limit` messages from the queue, if any. Returns empty list if the queue is empty.
     *
     * @param queue queue from which to receive a message
     * @param limit number of messages to receive
     * @return messages or an empty list if the queue is empty
     */
    fun <Data, Meta : Any> receive(queue: Queue<Data, Meta>, limit: Int): List<Message<Data, Meta>> {
        return receive(queue, limit, ReceiveOptions())
    }

    /**
     * Receives up to `limit` messages from the queue using custom filters, if any. Returns an empty list if no message
     * matches the filters.
     *
     * Filters can be specified using nice Kotlin lambda syntax as follows:
     * ```
     * // Try to read up to 100 messages with (userId<=10 OR userId=78) from the queue
     * val messages = consumer.receive(queue, 100) {
     *     // Type-safe DSL to filter messages
     *     (Metadata::userId lessEq 10) or (Metadata::userId eq 78)
     * }
     * ```
     *
     * If you use Java, please take a look at [JavaField][kolbasa.consumer.JavaField] class for examples.
     *
     * @param queue queue from which to receive a message
     * @param limit number of messages to receive
     * @param filter custom, user-defined filters to receive only specific messages from the queue
     * @return messages or an empty list if no message matches the filters
     */
    fun <Data, Meta : Any> receive(
        queue: Queue<Data, Meta>,
        limit: Int,
        filter: Filter.() -> Condition<Meta>
    ): List<Message<Data, Meta>> {
        return receive(queue, limit, ReceiveOptions(filter = filter(Filter)))
    }

    /**
     * Receives up to `limit` messages from the queue using custom [options][ReceiveOptions], if any. Returns an empty list
     * if no message matches the filters specified in the `receiveOptions` method parameter.
     *
     * Custom [ReceiveOptions] allows to specify custom filters, messages ordering, visibility timeout and so on. For
     * more details please read [ReceiveOptions] docs.

     * @param queue queue from which to receive a message
     * @param limit number of messages to receive
     * @param receiveOptions custom options (filters, ordering etc.)
     * @return messages or an empty list if no message matches the filters specified in the `receiveOptions`
     */
    fun <Data, Meta : Any> receive(
        queue: Queue<Data, Meta>,
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>
    ): List<Message<Data, Meta>>

    // Delete

    /**
     * Deletes the processed message from the queue.
     *
     * @param queue queue from which the message should be deleted
     * @param message message to delete
     */
    fun <Data, Meta : Any> delete(queue: Queue<Data, Meta>, message: Message<Data, Meta>): Int {
        return delete(queue, message.id)
    }

    /**
     * Deletes the processed messages from the queue.
     *
     * @param queue queue from which the messages should be deleted
     * @param messages messages to delete
     */
    fun <Data, Meta : Any> delete(queue: Queue<Data, Meta>, messages: Collection<Message<Data, Meta>>): Int {
        return delete(queue, messages.map(Message<Data, Meta>::id))
    }

    /**
     * Deletes the processed message from the queue by message [identifier][kolbasa.producer.Id]
     *
     * @param queue queue from which the message should be deleted
     * @param messageId identifier of the message to delete
     */
    fun <Data, Meta : Any> delete(queue: Queue<Data, Meta>, messageId: Id): Int {
        return delete(queue, listOf(messageId))
    }

    /**
     * Deletes the processed messages from the queue by messages [identifiers][kolbasa.producer.Id]
     *
     * @param queue queue from which the messages should be deleted
     * @param messageIds identifiers of the messages to delete
     */
    fun <Data, Meta : Any> delete(queue: Queue<Data, Meta>, messageIds: List<Id>): Int

}

