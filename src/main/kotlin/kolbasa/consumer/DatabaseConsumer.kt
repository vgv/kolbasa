package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseConsumer<Data, Meta : Any>(
    private val dataSource: DataSource,
    queue: Queue<Data, Meta>,
    consumerOptions: ConsumerOptions = ConsumerOptions()
) : Consumer<Data, Meta> {

    private val peer = ConnectionAwareDatabaseConsumer(queue, consumerOptions)

    override fun receive(): Message<Data, Meta>? {
        return dataSource.useConnection { peer.receive(it) }
    }

    override fun receive(filter: () -> Condition<Meta>): Message<Data, Meta>? {
        return dataSource.useConnection { peer.receive(it, filter) }
    }

    override fun receive(receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>? {
        return dataSource.useConnection { peer.receive(it, receiveOptions) }
    }

    override fun receive(limit: Int): List<Message<Data, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit) }
    }

    override fun receive(limit: Int, filter: () -> Condition<Meta>): List<Message<Data, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit, filter) }
    }

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit, receiveOptions) }
    }

    override fun delete(messageId: Long): Int {
        return dataSource.useConnection { peer.delete(it, messageId) }
    }

    override fun delete(messageIds: List<Long>): Int {
        return dataSource.useConnection { peer.delete(it, messageIds) }
    }

    override fun delete(message: Message<Data, Meta>): Int {
        return dataSource.useConnection { peer.delete(it, message) }
    }

    override fun delete(messages: Collection<Message<Data, Meta>>): Int {
        return dataSource.useConnection { peer.delete(it, messages) }
    }

}
