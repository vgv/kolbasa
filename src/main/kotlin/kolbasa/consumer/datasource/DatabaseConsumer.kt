package kolbasa.consumer.datasource

import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseConsumer<Data, Meta : Any>(
    private val dataSource: DataSource,
    private val peer: ConnectionAwareConsumer<Data, Meta>
) : Consumer<Data, Meta> {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        queue: Queue<Data, Meta>,
        consumerOptions: ConsumerOptions = ConsumerOptions()
    ) : this(
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseConsumer(queue, consumerOptions)
    )

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit, receiveOptions) }
    }

    override fun delete(messageIds: List<Long>): Int {
        return dataSource.useConnection { peer.delete(it, messageIds) }
    }

}
