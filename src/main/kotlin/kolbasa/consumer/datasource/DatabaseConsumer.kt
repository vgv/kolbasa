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

/**
 * Default implementation of [Consumer]
 */
class DatabaseConsumer internal constructor(
    private val dataSource: DataSource,
    private val peer: ConnectionAwareConsumer
) : Consumer {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        consumerOptions: ConsumerOptions = ConsumerOptions()
    ) : this(
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseConsumer(consumerOptions)
    )

    override fun <Data, Meta : Any> receive(queue: Queue<Data, Meta>, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        // Do we need to read OT data?
        receiveOptions.readOpenTelemetryData = queue.queueTracing.readOpenTelemetryData()

        return queue.queueTracing.makeConsumerCall {
            dataSource.useConnection { peer.receive(it, queue, limit, receiveOptions) }
        }
    }

    override fun <Data, Meta : Any> delete(queue: Queue<Data, Meta>, messageIds: List<Id>): Int {
        return dataSource.useConnection { peer.delete(it, queue, messageIds) }
    }

}
