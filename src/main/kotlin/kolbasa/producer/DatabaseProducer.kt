package kolbasa.producer

import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseProducer<V, Meta : Any>(
    private val dataSource: DataSource,
    queue: Queue<V, Meta>,
    producerOptions: ProducerOptions = ProducerOptions()
) : Producer<V, Meta> {

    private val peer = ConnectionAwareDatabaseProducer(queue, producerOptions)

    override fun send(data: V): Long {
        return dataSource.useConnection { peer.send(it, data) }
    }

    override fun send(data: SendMessage<V, Meta>): Long {
        return dataSource.useConnection { peer.send(it, data) }
    }

    override fun send(data: List<SendMessage<V, Meta>>): SendResult<V, Meta> {
        return dataSource.useConnection { peer.send(it, data) }
    }

}

