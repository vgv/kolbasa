package kolbasa.consumer.datasource

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.producer.Id
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseConsumer<Data, Meta : Any>(
    private val dataSource: DataSource,
    private val queue: Queue<Data, Meta>,
    private val peer: ConnectionAwareConsumer
) : Consumer<Data, Meta> {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        queue: Queue<Data, Meta>,
        consumerOptions: ConsumerOptions = ConsumerOptions()
    ) : this(
        dataSource = dataSource,
        queue = queue,
        peer = ConnectionAwareDatabaseConsumer(consumerOptions)
    )

    override fun <D, M : Any> receive(queue: Queue<D, M>, limit: Int, receiveOptions: ReceiveOptions<M>): List<Message<D, M>> {
        // Do we need to read OT data?
        receiveOptions.readOpenTelemetryData = queue.queueTracing.readOpenTelemetryData()

        return queue.queueTracing.makeConsumerCall {
            dataSource.useConnection { peer.receive(it, queue, limit, receiveOptions) }
        }
    }

    override fun <D, M : Any> delete(queue: Queue<D, M>, messageIds: List<Id>): Int {
        return dataSource.useConnection { peer.delete(it, queue, messageIds) }
    }

    // -----------------------------------------------------------------------------------------------------

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        return receive(queue, limit, receiveOptions)
    }

    override fun delete(messageIds: List<Id>): Int {
        return delete(queue, messageIds)
    }
}
