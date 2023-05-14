package kolbasa.producer

import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.queue.Queue
import javax.sql.DataSource

class DatabaseProducer<V, M : Any>(
    private val dataSource: DataSource,
    queue: Queue<V, M>,
    producerOptions: ProducerOptions = ProducerOptions()
) : Producer<V, M> {

    private val peer = ConnectionAwareDatabaseProducer(queue, producerOptions)

    override fun send(data: V): Long {
        return dataSource.useConnection { peer.send(it, data) }
    }

    override fun send(data: SendMessage<V, M>): Long {
        return dataSource.useConnection { peer.send(it, data) }
    }

    override fun send(data: List<SendMessage<V, M>>): SendResult<V, M> {
        return dataSource.useConnection { peer.send(it, data) }
    }

}

