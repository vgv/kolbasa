package kolbasa.producer

import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.queue.Queue
import javax.sql.DataSource

/**
 * Default implementation of [Producer]
 */
class DatabaseProducer<Data, Meta : Any>(
    private val dataSource: DataSource,
    queue: Queue<Data, Meta>,
    producerOptions: ProducerOptions = ProducerOptions()
) : Producer<Data, Meta> {

    private val peer: ConnectionAwareProducer<Data, Meta> = ConnectionAwareDatabaseProducer(queue, producerOptions)

    override fun send(data: Data): Long {
        return dataSource.useConnection { peer.send(it, data) }
    }

    override fun send(data: SendMessage<Data, Meta>): Long {
        return dataSource.useConnection { peer.send(it, data) }
    }

    override fun send(data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta> {
        return dataSource.useConnection { peer.send(it, data) }
    }

    override fun send(data: List<SendMessage<Data, Meta>>, sendOptions: SendOptions): SendResult<Data, Meta> {
        return dataSource.useConnection { peer.send(it, data, sendOptions) }
    }
}

