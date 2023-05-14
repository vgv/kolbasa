package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseConsumer<V, M : Any>(
    private val dataSource: DataSource,
    queue: Queue<V, M>,
    consumerOptions: ConsumerOptions = ConsumerOptions()
) : Consumer<V, M> {

    private val peer = ConnectionAwareDatabaseConsumer(queue, consumerOptions)

    override fun receive(): Message<V, M>? {
        return dataSource.useConnection { peer.receive(it) }
    }

    override fun receive(filter: () -> Condition<M>): Message<V, M>? {
        return dataSource.useConnection { peer.receive(it, filter) }
    }

    override fun receive(receiveOptions: ReceiveOptions<M>): Message<V, M>? {
        return dataSource.useConnection { peer.receive(it, receiveOptions) }
    }

    override fun receive(limit: Int): List<Message<V, M>> {
        return dataSource.useConnection { peer.receive(it, limit) }
    }

    override fun receive(limit: Int, filter: () -> Condition<M>): List<Message<V, M>> {
        return dataSource.useConnection { peer.receive(it, limit, filter) }
    }

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<M>): List<Message<V, M>> {
        return dataSource.useConnection { peer.receive(it, limit, receiveOptions) }
    }

    override fun delete(messageId: Long): Int {
        return dataSource.useConnection { peer.delete(it, messageId) }
    }

    override fun delete(messageIds: List<Long>): Int {
        return dataSource.useConnection { peer.delete(it, messageIds) }
    }

    override fun delete(message: Message<V, M>): Int {
        return dataSource.useConnection { peer.delete(it, message) }
    }

    override fun delete(messages: Collection<Message<V, M>>): Int {
        return dataSource.useConnection { peer.delete(it, messages) }
    }

}
