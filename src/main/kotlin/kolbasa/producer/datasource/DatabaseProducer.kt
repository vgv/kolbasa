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
class DatabaseProducer(
    private val dataSource: DataSource,
    private val peer: ConnectionAwareProducer
) : Producer {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        producerOptions: ProducerOptions = ProducerOptions(),
    ) : this(
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseProducer(producerOptions)
    )

    override fun <Data, Meta : Any> send(queue: Queue<Data, Meta>, request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        return queue.queueTracing.makeProducerCall(request) {
            dataSource.useConnection { peer.send(it, queue, request) }
        }
    }

}

