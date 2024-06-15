package kolbasa.consumer.connection

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.producer.Id
import java.sql.Connection

interface ConnectionAwareConsumer<Data, Meta : Any> {

    fun receive(connection: Connection): Message<Data, Meta>? {
        return receive(connection, ReceiveOptions())
    }

    fun receive(connection: Connection, filter: Filter.() -> Condition<Meta>): Message<Data, Meta>? {
        return receive(connection, ReceiveOptions(filter = filter(Filter)))
    }

    fun receive(connection: Connection, receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>? {
        val result = receive(connection, limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    fun receive(connection: Connection, limit: Int): List<Message<Data, Meta>> {
        return receive(connection, limit, ReceiveOptions())
    }

    fun receive(connection: Connection, limit: Int, filter: Filter.() -> Condition<Meta>): List<Message<Data, Meta>> {
        return receive(connection, limit, ReceiveOptions(filter = filter(Filter)))
    }

    fun receive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>>

    // Delete

    fun delete(connection: Connection, message: Message<Data, Meta>): Int {
        return delete(connection, message.id)
    }

    fun delete(connection: Connection, messages: Collection<Message<Data, Meta>>): Int {
        return delete(connection, messages.map(Message<Data, Meta>::id))
    }

    fun delete(connection: Connection, messageId: Id): Int {
        return delete(connection, listOf(messageId))
    }

    fun delete(connection: Connection, messageIds: List<Id>): Int
}
