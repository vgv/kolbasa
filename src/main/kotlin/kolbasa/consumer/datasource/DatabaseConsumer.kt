package kolbasa.consumer.datasource

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Default implementation of [Consumer]
 */
class DatabaseConsumer internal constructor(
    private val nodeId: NodeId,
    private val dataSource: DataSource,
    private val peer: ConnectionAwareConsumer
) : Consumer {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        consumerOptions: ConsumerOptions = ConsumerOptions.DEFAULT
    ) : this(
        nodeId = NodeId.EMPTY_NODE_ID,
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseConsumer(consumerOptions)
    )

    override fun <Data> receive(queue: Queue<Data>, limit: Int, receiveOptions: ReceiveOptions): List<Message<Data>> {
        // Do we need to read OT data?
        receiveOptions.readOpenTelemetryData = queue.queueTracing.readOpenTelemetryData()

        return queue.queueTracing.makeConsumerCall(nodeId) {
            dataSource.useConnection { peer.receive(it, queue, limit, receiveOptions) }
        }
    }

    override fun <Data> delete(queue: Queue<Data>, messageIds: List<Id>): Int {
        return dataSource.useConnection { peer.delete(it, queue, messageIds) }
    }

}
