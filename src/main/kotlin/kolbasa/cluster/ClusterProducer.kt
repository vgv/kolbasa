package kolbasa.cluster

import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.producer.datasource.ProducerInterceptor
import kolbasa.queue.Queue
import java.util.SortedMap
import javax.sql.DataSource

class ClusterProducer<Data, Meta : Any>(
    private val dataSources: DataSourcesStorage,
    private val queue: Queue<Data, Meta>,
    private val producerOptions: ProducerOptions = ProducerOptions(),
    private val interceptors: List<ProducerInterceptor<Data, Meta>> = emptyList(),
) : Producer<Data, Meta> {

    @Volatile
    private var lastValue: SortedMap<NodeInfo, DataSource> = sortedMapOf()

    @Volatile
    private var producers: List<Producer<Data, Meta>> = emptyList()

    override fun send(request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        updateProducers()
        val producer = producers.random()
        return producer.send(request)
    }

    private fun updateProducers() {
        val newDataSources = dataSources.readyToSendDataSources

        if (newDataSources !== lastValue) {
            producers = newDataSources.map { (clusterInfo, dataSource) ->
                val connectionAwareProducer = ConnectionAwareDatabaseProducer(
                    queue = queue,
                    producerOptions = producerOptions,
                    interceptors = emptyList(),
                    serverId = clusterInfo.serverId
                )

                DatabaseProducer(dataSource, connectionAwareProducer, interceptors)
            }

            lastValue = newDataSources
        }
    }

}
