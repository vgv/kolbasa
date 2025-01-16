package kolbasa.consumer.datasource

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.producer.Id
import kolbasa.queue.Queue

interface Consumer<Data, Meta : Any> {

    fun <D, M: Any> receive(queue: Queue<D, M>): Message<D, M>? {
        return receive(queue, ReceiveOptions())
    }

    fun <D, M: Any> receive(queue: Queue<D, M>, filter: Filter.() -> Condition<M>): Message<D, M>? {
        return receive(queue, ReceiveOptions(filter = filter(Filter)))
    }

    fun <D, M: Any> receive(queue: Queue<D, M>, receiveOptions: ReceiveOptions<M>): Message<D, M>? {
        val result = receive(queue, limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    fun <D, M: Any> receive(queue: Queue<D, M>, limit: Int): List<Message<D, M>> {
        return receive(queue, limit, ReceiveOptions())
    }

    fun <D, M: Any> receive(queue: Queue<D, M>, limit: Int, filter: Filter.() -> Condition<M>): List<Message<D, M>> {
        return receive(queue, limit, ReceiveOptions(filter = filter(Filter)))
    }

    fun <D, M: Any> receive(queue: Queue<D, M>, limit: Int, receiveOptions: ReceiveOptions<M>): List<Message<D, M>>

    // Delete

    fun <D, M: Any> delete(queue: Queue<D, M>, message: Message<D, M>): Int {
        return delete(queue, message.id)
    }

    fun <D, M: Any> delete(queue: Queue<D, M>, messages: Collection<Message<D, M>>): Int {
        return delete(queue, messages.map(Message<D, M>::id))
    }

    fun <D, M: Any> delete(queue: Queue<D, M>, messageId: Id): Int {
        return delete(queue, listOf(messageId))
    }

    fun <D, M: Any> delete(queue: Queue<D, M>, messageIds: List<Id>): Int

    // -----------------------------------------------------------------------------------------------------

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

    fun delete(messageId: Id): Int {
        return delete(listOf(messageId))
    }

    fun delete(messageIds: List<Id>): Int

}

