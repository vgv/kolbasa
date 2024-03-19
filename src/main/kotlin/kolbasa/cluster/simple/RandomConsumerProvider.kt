package kolbasa.cluster.simple

import kolbasa.consumer.Consumer
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.DatabaseConsumer
import kolbasa.queue.Queue
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource
import kotlin.random.Random

class RandomConsumerProvider(
    private val dataSources: List<DataSource>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions()
) : ConsumerProvider {

    private val consumers: MutableMap<Queue<*, *>, List<Consumer<*, *>>> = ConcurrentHashMap()

    init {
        check(dataSources.isNotEmpty()) {
            "You have to provide at least one DataSource"
        }
    }

    override fun <Data, Meta : Any> consumer(queue: Queue<Data, Meta>): Consumer<Data, Meta> {
        val queueConsumers = consumers.computeIfAbsent(queue) {
            generateConsumers(queue)
        }

        @Suppress("UNCHECKED_CAST")
        return queueConsumers.random() as Consumer<Data, Meta>
    }

    override fun <Data, Meta : Any> consumer(queue: Queue<Data, Meta>, shard: Int): Consumer<Data, Meta> {
        val queueConsumers = consumers.computeIfAbsent(queue) {
            generateConsumers(queue)
        }

        @Suppress("UNCHECKED_CAST")
        return if (Random.nextInt(RANDOM_CONSUMER_PROBABILITY) == 0) {
            queueConsumers.random()
        } else {
            val index = shard % queueConsumers.size
            queueConsumers[index]
        } as Consumer<Data, Meta>
    }

    private fun <Data, Meta : Any> generateConsumers(queue: Queue<Data, Meta>): List<Consumer<Data, Meta>> {
        return dataSources.map { dataSource ->
            DatabaseConsumer(dataSource, queue, consumerOptions)
        }
    }

    internal companion object {
        const val RANDOM_CONSUMER_PROBABILITY = 20  // 20 means 1/20
    }

}
