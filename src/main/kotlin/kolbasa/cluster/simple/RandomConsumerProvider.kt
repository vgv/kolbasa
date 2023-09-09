package kolbasa.cluster.simple

import kolbasa.consumer.Consumer
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.DatabaseConsumer
import kolbasa.queue.Queue
import javax.sql.DataSource

class RandomConsumerProvider<Data, Meta : Any>(
    dataSources: List<DataSource>,
    queue: Queue<Data, Meta>,
    consumerOptions: ConsumerOptions = ConsumerOptions()
) : ConsumerProvider<Data, Meta> {

    private val consumers = dataSources.map { dataSource ->
        DatabaseConsumer(dataSource, queue, consumerOptions)
    }

    override fun consumer(): Consumer<Data, Meta> {
        return consumers.random()
    }

}
