package kolbasa.cluster

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.Consumer
import kolbasa.consumer.datasource.ConsumerInterceptor
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.util.*
import javax.sql.DataSource

class ClusterConsumer<Data, Meta : Any>(
    private val queue: Queue<Data, Meta>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions(),
    private val interceptors: List<ConsumerInterceptor<Data, Meta>> = emptyList(),
) : Consumer<Data, Meta> {

    @Volatile
    private var lastValue: SortedMap<String, DataSource> = sortedMapOf()

    @Volatile
    private var consumers: Map<String, Consumer<Data, Meta>> = emptyMap()

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        updateConsumers()

        return if (consumers.isEmpty()) {
            emptyList()
        } else {
            val randomConsumer = consumers.entries.random().value
            randomConsumer.receive(limit, receiveOptions)
        }
    }

    override fun delete(messageIds: List<Id>): Int {
        updateConsumers()

        val byServer = messageIds.groupBy {
            it.serverId ?: throw IllegalArgumentException("No known server with serverId=null")
        }

        val results = byServer.map { (serverId, ids) ->
            val consumer = consumers[serverId] ?: throw IllegalArgumentException("No known server with serverId=$serverId")
            consumer.delete(ids)
        }

        return results.sum()
    }

    private fun updateConsumers() {
//        val newDataSources = dataSources.readyToReceiveDataSources
//
//        if (newDataSources !== lastValue) {
//            consumers = newDataSources.entries.associate { (clusterInfo, dataSource) ->
//                val connectionAwareConsumer = ConnectionAwareDatabaseConsumer(
//                    queue = queue,
//                    consumerOptions = consumerOptions,
//                    interceptors = emptyList(),
//                    serverId = clusterInfo.serverId
//                )
//
//                clusterInfo.serverId to DatabaseConsumer(dataSource, connectionAwareConsumer, interceptors)
//            }
//
//            lastValue = newDataSources
//        }
    }
}
