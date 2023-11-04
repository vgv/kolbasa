package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import java.sql.Connection

interface Consumer<Data, Meta : Any> {
    fun receive(): Message<Data, Meta>?
    fun receive(filter: Filter.() -> Condition<Meta>): Message<Data, Meta>?
    fun receive(receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>?

    fun receive(limit: Int): List<Message<Data, Meta>>
    fun receive(limit: Int, filter: Filter.() -> Condition<Meta>): List<Message<Data, Meta>>
    fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>>

    fun delete(messageId: Long): Int
    fun delete(messageIds: List<Long>): Int
    fun delete(message: Message<Data, Meta>): Int
    fun delete(messages: Collection<Message<Data, Meta>>): Int
}

interface ConnectionAwareConsumer<Data, Meta : Any> {
    fun receive(connection: Connection): Message<Data, Meta>?
    fun receive(connection: Connection, filter: Filter.() -> Condition<Meta>): Message<Data, Meta>?
    fun receive(connection: Connection, receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>?

    fun receive(connection: Connection, limit: Int): List<Message<Data, Meta>>
    fun receive(connection: Connection, limit: Int, filter: Filter.() -> Condition<Meta>): List<Message<Data, Meta>>
    fun receive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>>

    fun delete(connection: Connection, messageId: Long): Int
    fun delete(connection: Connection, messageIds: List<Long>): Int
    fun delete(connection: Connection, message: Message<Data, Meta>): Int
    fun delete(connection: Connection, messages: Collection<Message<Data, Meta>>): Int
}
