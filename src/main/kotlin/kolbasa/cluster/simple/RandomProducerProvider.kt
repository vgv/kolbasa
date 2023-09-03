package kolbasa.cluster.simple

import kolbasa.producer.DatabaseProducer
import kolbasa.producer.Producer
import kolbasa.producer.ProducerOptions
import kolbasa.queue.Queue
import javax.sql.DataSource

class RandomProducerProvider<Data, Meta : Any>(
    dataSources: List<DataSource>,
    queue: Queue<Data, Meta>,
    producerOptions: ProducerOptions = ProducerOptions()
) : ProducerProvider<Data, Meta> {

    private val producers = dataSources.map { dataSource ->
        DatabaseProducer(dataSource, queue, producerOptions)
    }

    override fun producer(): Producer<Data, Meta> {
        return producers.random()
    }
}
