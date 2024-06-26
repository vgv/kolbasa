package kolbasa.cluster.simple

import kolbasa.producer.ProducerOptions
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.queue.Queue
import kolbasa.stats.opentelemetry.TracingProducerInterceptor
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

    override fun <Data, Meta : Any> producer(queue: Queue<Data, Meta>, shard: Int): Producer<Data, Meta> {
        val queueProducers = producers.computeIfAbsent(queue) {
            generateProducers(queue)
        }

        val index = shard % queueProducers.size
        @Suppress("UNCHECKED_CAST")
        return queueProducers[index] as Producer<Data, Meta>
    }

    private fun <Data, Meta : Any> generateProducers(queue: Queue<Data, Meta>): List<Producer<Data, Meta>> {
        return dataSources.map { dataSource ->
            val tracing = TracingProducerInterceptor(queue)
            DatabaseProducer(dataSource, queue, producerOptions, listOf(tracing))
        }
    }
}
