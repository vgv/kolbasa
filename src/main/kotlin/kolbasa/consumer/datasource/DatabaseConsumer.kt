package kolbasa.consumer.datasource

import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.connection.ConnectionAwareConsumer
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.producer.Id
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseConsumer<Data, Meta : Any> @JvmOverloads constructor(
    private val dataSource: DataSource,
    private val peer: ConnectionAwareConsumer<Data, Meta>,
    private val interceptors: List<ConsumerInterceptor<Data, Meta>> = emptyList()
) : Consumer<Data, Meta> {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        queue: Queue<Data, Meta>,
        consumerOptions: ConsumerOptions = ConsumerOptions(),
        interceptors: List<ConsumerInterceptor<Data, Meta>> = emptyList()
    ) : this(
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseConsumer(queue, consumerOptions),
        interceptors = interceptors
    )

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        return ConsumerInterceptor.recursiveApplyReceiveInterceptors(interceptors, limit, receiveOptions) { lmt, rcvOptions ->
            doRealReceive(lmt, rcvOptions)
        }
    }

    private fun doRealReceive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        return dataSource.useConnection { peer.receive(it, limit, receiveOptions) }
    }

    override fun delete(messageIds: List<Id>): Int {
        return ConsumerInterceptor.recursiveApplyDeleteInterceptors(interceptors, messageIds) { msgIds ->
            doRealDelete(msgIds)
        }
    }

    private fun doRealDelete(messageIds: List<Id>): Int {
        return dataSource.useConnection { peer.delete(it, messageIds) }
    }

}
