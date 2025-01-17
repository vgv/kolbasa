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

    @Deprecated("Use receive(queue) instead", ReplaceWith("receive(queue)"))
    fun receive(): Message<Data, Meta>? {
        return receive(ReceiveOptions())
    }

    @Deprecated("Use receive(queue, filter) instead", ReplaceWith("receive(queue, filter)"))
    fun receive(filter: Filter.() -> Condition<Meta>): Message<Data, Meta>? {
        return receive(ReceiveOptions(filter = filter(Filter)))
    }

    @Deprecated("Use receive(queue, receiveOptions) instead", ReplaceWith("receive(queue, receiveOptions)"))
    fun receive(receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>? {
        val result = receive(limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    @Deprecated("Use receive(queue, limit) instead", ReplaceWith("receive(queue, limit)"))
    fun receive(limit: Int): List<Message<Data, Meta>> {
        return receive(limit, ReceiveOptions())
    }

    @Deprecated("Use receive(queue, limit, filter) instead", ReplaceWith("receive(queue, limit, filter)"))
    fun receive(limit: Int, filter: Filter.() -> Condition<Meta>): List<Message<Data, Meta>> {
        return receive(limit, ReceiveOptions(filter = filter(Filter)))
    }

    @Deprecated("Use receive(queue, limit, receiveOptions) instead", ReplaceWith("receive(queue, limit, receiveOptions)"))
    fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>>

    // Delete
    @Deprecated("Use delete(queue, message) instead", ReplaceWith("delete(queue, message)"))
    fun delete(message: Message<Data, Meta>): Int {
        return delete(message.id)
    }

    @Deprecated("Use delete(queue, messages) instead", ReplaceWith("delete(queue, messages)"))
    fun delete(messages: Collection<Message<Data, Meta>>): Int {
        return delete(messages.map(Message<Data, Meta>::id))
    }

    @Deprecated("Use delete(queue, messageId) instead", ReplaceWith("delete(queue, messageId)"))
    fun delete(messageId: Id): Int {
        return delete(listOf(messageId))
    }

    @Deprecated("Use delete(queue, messageIds) instead", ReplaceWith("delete(queue, messageIds)"))
    fun delete(messageIds: List<Id>): Int

}

