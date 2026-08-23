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

    /**
     * Creates a consumer that manages connections and transactions itself.
     *
     * Every call takes a connection from `dataSource`, does its work in one transaction and gives the connection
     * back. You never open, commit, roll back or close anything.
     *
     * Because `receive()` and `delete()` are separate calls, they are separate transactions: a message received here
     * stays invisible to other consumers until its visibility timeout ends, and it is really gone only after
     * `delete()`. If you need the message and your own database changes to be one atomic operation, use
     * [ConnectionAwareDatabaseConsumer][kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer].
     *
     * The consumer is thread-safe and holds no state between calls, so create one per set of defaults and share it.
     *
     * @param dataSource the pool this consumer takes connections from
     * @param consumerOptions defaults for every call of this consumer. Without it, [ConsumerOptions.DEFAULT] is used
     * and every call follows the queue defaults.
     */
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
