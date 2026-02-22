package kolbasa.producer.datasource

import kolbasa.Kolbasa
import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.producer.ProducerOptions
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import java.util.concurrent.CompletableFuture
import javax.sql.DataSource

/**
 * Default implementation of [Producer]
 */
class DatabaseProducer internal constructor(
    private val nodeId: NodeId,
    private val dataSource: DataSource,
    private val peer: ConnectionAwareProducer
): Producer {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        producerOptions: ProducerOptions = ProducerOptions.DEFAULT
    ) : this(
        nodeId = NodeId.EMPTY_NODE_ID,
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseProducer(producerOptions)
    )

    override fun <Data> send(queue: Queue<Data>, request: SendRequest<Data>): SendResult<Data> {
        return queue.queueTracing.makeProducerCall(nodeId, request) {
            dataSource.useConnection { peer.send(it, queue, request) }
        }
    }

    override fun <Data> sendAsync(
        queue: Queue<Data>,
        request: SendRequest<Data>
    ): CompletableFuture<SendResult<Data>> {
        val executor = ProducerSchemaHelpers.calculateAsyncExecutor(
            callExecutor = request.sendOptions.asyncExecutor,
            // make it better somehow
            producerExecutor = (peer as? ConnectionAwareDatabaseProducer)?.producerOptions?.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ send(queue, request) }, executor)
    }
}

