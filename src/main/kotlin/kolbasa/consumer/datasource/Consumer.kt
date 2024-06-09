package kolbasa.consumer.datasource

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter

interface Consumer<Data, Meta : Any> {

    fun receive(): Message<Data, Meta>? {
        return receive(ReceiveOptions())
    }

    fun receive(filter: Filter.() -> Condition<Meta>): Message<Data, Meta>? {
        return receive(ReceiveOptions(filter = filter(Filter)))
    }

    fun receive(receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>? {
        val result = receive(limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    fun receive(limit: Int): List<Message<Data, Meta>> {
        return receive(limit, ReceiveOptions())
    }

    fun receive(limit: Int, filter: Filter.() -> Condition<Meta>): List<Message<Data, Meta>> {
        return receive(limit, ReceiveOptions(filter = filter(Filter)))
    }

    fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>>

    // Delete

    fun delete(message: Message<Data, Meta>): Int {
        return delete(message.id)
    }

    fun delete(messages: Collection<Message<Data, Meta>>): Int {
        return delete(messages.map(Message<Data, Meta>::id))
    }

    fun delete(messageId: Long): Int {
        return delete(listOf(messageId))
    }

    fun delete(messageIds: List<Long>): Int

}

