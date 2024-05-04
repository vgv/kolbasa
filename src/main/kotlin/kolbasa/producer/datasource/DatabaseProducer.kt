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
class DatabaseProducer<Data, Meta : Any> @JvmOverloads constructor(
    private val dataSource: DataSource,
    private val peer: ConnectionAwareProducer<Data, Meta>,
    private val interceptors: List<ProducerInterceptor<Data, Meta>> = emptyList(),
) : Producer<Data, Meta> {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        queue: Queue<Data, Meta>,
        producerOptions: ProducerOptions = ProducerOptions(),
        interceptors: List<ProducerInterceptor<Data, Meta>> = emptyList(),
    ) : this(
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseProducer(queue, producerOptions),
        interceptors = interceptors
    )

    override fun send(request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        return ProducerInterceptor.recursiveApplyInterceptors(interceptors, request) { req ->
            doRealSend(req)
        }
    }

    private fun doRealSend(request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        return dataSource.useConnection { peer.send(it, request) }
    }
}

