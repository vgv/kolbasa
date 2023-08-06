package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseConsumer<V, Meta : Any>(
    private val dataSource: DataSource,
    queue: Queue<V, Meta>,
    consumerOptions: ConsumerOptions = ConsumerOptions()
) : Consumer<V, Meta> {

    private val peer = ConnectionAwareDatabaseConsumer(queue, consumerOptions)

    override fun receive(): Message<V, Meta>? {
        return dataSource.useConnection { peer.receive(it) }
    }

    override fun receive(filter: () -> Condition<Meta>): Message<V, Meta>? {
        return dataSource.useConnection { peer.receive(it, filter) }
    }

    override fun receive(receiveOptions: ReceiveOptions<Meta>): Message<V, Meta>? {
        return dataSource.useConnection { peer.receive(it, receiveOptions) }
    }

    override fun receive(limit: Int): List<Message<V, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit) }
    }

    override fun receive(limit: Int, filter: () -> Condition<Meta>): List<Message<V, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit, filter) }
    }

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<V, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit, receiveOptions) }
    }

    override fun delete(messageId: Long): Int {
        return dataSource.useConnection { peer.delete(it, messageId) }
    }

    override fun delete(messageIds: List<Long>): Int {
        return dataSource.useConnection { peer.delete(it, messageIds) }
    }

    override fun delete(message: Message<V, Meta>): Int {
        return dataSource.useConnection { peer.delete(it, message) }
    }

    override fun delete(messages: Collection<Message<V, Meta>>): Int {
        return dataSource.useConnection { peer.delete(it, messages) }
    }

}
