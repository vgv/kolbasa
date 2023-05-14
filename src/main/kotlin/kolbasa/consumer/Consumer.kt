package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import java.sql.Connection

interface Consumer<V, M : Any> {
    fun receive(): Message<V, M>?
    fun receive(filter: () -> Condition<M>): Message<V, M>?
    fun receive(receiveOptions: ReceiveOptions<M>): Message<V, M>?

    fun receive(limit: Int): List<Message<V, M>>
    fun receive(limit: Int, filter: () -> Condition<M>): List<Message<V, M>>
    fun receive(limit: Int, receiveOptions: ReceiveOptions<M>): List<Message<V, M>>

    fun delete(messageId: Long): Int
    fun delete(messageIds: List<Long>): Int
    fun delete(message: Message<V, M>): Int
    fun delete(messages: Collection<Message<V, M>>): Int
}

interface ConnectionAwareConsumer<V, M : Any> {
    fun receive(connection: Connection): Message<V, M>?
    fun receive(connection: Connection, filter: () -> Condition<M>): Message<V, M>?
    fun receive(connection: Connection, receiveOptions: ReceiveOptions<M>): Message<V, M>?

    fun receive(connection: Connection, limit: Int): List<Message<V, M>>
    fun receive(connection: Connection, limit: Int, filter: () -> Condition<M>): List<Message<V, M>>
    fun receive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<M>): List<Message<V, M>>

    fun delete(connection: Connection, messageId: Long): Int
    fun delete(connection: Connection, messageIds: List<Long>): Int
    fun delete(connection: Connection, message: Message<V, M>): Int
    fun delete(connection: Connection, messages: Collection<Message<V, M>>): Int
}
