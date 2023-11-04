package kolbasa.cluster.simple

import kolbasa.producer.DatabaseProducer
import kolbasa.producer.Producer
import kolbasa.producer.ProducerOptions
import kolbasa.queue.Queue
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

class RandomProducerProvider(
    private val dataSources: List<DataSource>,
    private val producerOptions: ProducerOptions = ProducerOptions()
) : ProducerProvider {

    private val producers: MutableMap<Queue<*, *>, List<Producer<*, *>>> = ConcurrentHashMap()

    init {
        check(dataSources.isNotEmpty()) {
            "You have to provide at least one DataSource"
        }
    }

    override fun <Data, Meta : Any> producer(queue: Queue<Data, Meta>): Producer<Data, Meta> {
        val queueProducers = producers.computeIfAbsent(queue) {
            generateProducers(queue)
        }

        @Suppress("UNCHECKED_CAST")
        return queueProducers.random() as Producer<Data, Meta>
    }

    private fun <Data, Meta : Any> generateProducers(queue: Queue<Data, Meta>): List<Producer<Data, Meta>> {
        return dataSources.map { dataSource ->
            DatabaseProducer(dataSource, queue, producerOptions)
        }
    }
}
