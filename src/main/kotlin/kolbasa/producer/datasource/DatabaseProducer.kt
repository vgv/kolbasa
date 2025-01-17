package kolbasa.producer.datasource

import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.connection.ConnectionAwareProducer
import kolbasa.queue.Queue
import javax.sql.DataSource

/**
 * Default implementation of [Producer]
 */
class DatabaseProducer<Data, Meta : Any>(
    private val dataSource: DataSource,
    private val queue: Queue<Data, Meta>,
    private val peer: ConnectionAwareProducer
) : Producer<Data, Meta> {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        queue: Queue<Data, Meta>,
        producerOptions: ProducerOptions = ProducerOptions(),
    ) : this(
        dataSource = dataSource,
        queue = queue,
        peer = ConnectionAwareDatabaseProducer(producerOptions)
    )

    override fun <D, M : Any> send(queue: Queue<D, M>, request: SendRequest<D, M>): SendResult<D, M> {
        return queue.queueTracing.makeProducerCall(request) {
            dataSource.useConnection { peer.send(it, queue, request) }
        }
    }

    override fun send(request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        return send(queue, request)
    }
}

