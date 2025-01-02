package kolbasa.cluster

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.consumer.datasource.Consumer
import kolbasa.consumer.datasource.ConsumerInterceptor
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.Id
import kolbasa.queue.Queue

class ClusterConsumer<Data, Meta : Any>(
    private val cluster: Cluster,
    private val queue: Queue<Data, Meta>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions(),
    private val interceptors: List<ConsumerInterceptor<Data, Meta>> = emptyList(),
) : Consumer<Data, Meta> {

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        val consumer = cluster.getState().getActiveConsumer(this) { dataSource, shards ->
            val c = ConnectionAwareDatabaseConsumer(queue, consumerOptions, emptyList(), shards)
            DatabaseConsumer(dataSource, c, interceptors)
        }

        return if (consumer == null) {
            emptyList()
        } else {
            consumer.receive(limit, receiveOptions)
        }
    }

    override fun delete(messageIds: List<Id>): Int {
        val byShard = messageIds.groupBy {
            it.shard
        }

        val state = cluster.getState()
        val results = byShard.map { (shard, ids) ->
            val consumer = state.getConsumer(this, shard) { dataSource ->
                DatabaseConsumer(dataSource, queue, consumerOptions, interceptors)
            }

            consumer.delete(ids)
        }

        return results.sum()
    }
}
