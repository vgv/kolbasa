package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import java.sql.Connection

interface Consumer<V, Meta : Any> {
    fun receive(): Message<V, Meta>?
    fun receive(filter: () -> Condition<Meta>): Message<V, Meta>?
    fun receive(receiveOptions: ReceiveOptions<Meta>): Message<V, Meta>?

    fun receive(limit: Int): List<Message<V, Meta>>
    fun receive(limit: Int, filter: () -> Condition<Meta>): List<Message<V, Meta>>
    fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<V, Meta>>

    fun delete(messageId: Long): Int
    fun delete(messageIds: List<Long>): Int
    fun delete(message: Message<V, Meta>): Int
    fun delete(messages: Collection<Message<V, Meta>>): Int
}

interface ConnectionAwareConsumer<V, Meta : Any> {
    fun receive(connection: Connection): Message<V, Meta>?
    fun receive(connection: Connection, filter: () -> Condition<Meta>): Message<V, Meta>?
    fun receive(connection: Connection, receiveOptions: ReceiveOptions<Meta>): Message<V, Meta>?

    fun receive(connection: Connection, limit: Int): List<Message<V, Meta>>
    fun receive(connection: Connection, limit: Int, filter: () -> Condition<Meta>): List<Message<V, Meta>>
    fun receive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<V, Meta>>

    fun delete(connection: Connection, messageId: Long): Int
    fun delete(connection: Connection, messageIds: List<Long>): Int
    fun delete(connection: Connection, message: Message<V, Meta>): Int
    fun delete(connection: Connection, messages: Collection<Message<V, Meta>>): Int
}
