package kolbasa.consumer.connection

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.sql.Connection

interface ConnectionAwareConsumer {

    fun <Data, Meta : Any> receive(connection: Connection, queue: Queue<Data, Meta>): Message<Data, Meta>? {
        return receive(connection, queue, ReceiveOptions())
    }

    fun <Data, Meta : Any> receive(connection: Connection, queue: Queue<Data, Meta>, filter: Filter.() -> Condition<Meta>): Message<Data, Meta>? {
        return receive(connection, queue, ReceiveOptions(filter = filter(Filter)))
    }

    fun <Data, Meta : Any> receive(connection: Connection, queue: Queue<Data, Meta>, receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>? {
        val result = receive(connection, queue, limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    fun <Data, Meta : Any> receive(connection: Connection, queue: Queue<Data, Meta>, limit: Int): List<Message<Data, Meta>> {
        return receive(connection, queue, limit, ReceiveOptions())
    }

    fun <Data, Meta : Any> receive(connection: Connection, queue: Queue<Data, Meta>, limit: Int, filter: Filter.() -> Condition<Meta>): List<Message<Data, Meta>> {
        return receive(connection, queue, limit, ReceiveOptions(filter = filter(Filter)))
    }

    fun <Data, Meta : Any> receive(connection: Connection, queue: Queue<Data, Meta>, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>>

    // Delete

    fun <Data, Meta : Any> delete(connection: Connection, queue: Queue<Data, Meta>, message: Message<Data, Meta>): Int {
        return delete(connection, queue, message.id)
    }

    fun <Data, Meta : Any> delete(connection: Connection, queue: Queue<Data, Meta>, messages: Collection<Message<Data, Meta>>): Int {
        return delete(connection, queue, messages.map(Message<Data, Meta>::id))
    }

    fun <Data, Meta : Any> delete(connection: Connection, queue: Queue<Data, Meta>, messageId: Id): Int {
        return delete(connection, queue, listOf(messageId))
    }

    fun <Data, Meta : Any> delete(connection: Connection, queue: Queue<Data, Meta>, messageIds: List<Id>): Int
}
