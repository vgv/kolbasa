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
import javax.sql.DataSource

class ClusterConsumer<Data, Meta : Any>(
    private val cluster: Cluster,
    private val queue: Queue<Data, Meta>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions(),
    private val interceptors: List<ConsumerInterceptor<Data, Meta>> = emptyList(),
) : Consumer<Data, Meta> {

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        val latestState = cluster.getState()

        val consumer = latestState.getActiveConsumer(this) { dataSource, shards ->
            val c = ConnectionAwareDatabaseConsumer(queue, consumerOptions, emptyList(), shards)
            DatabaseConsumer(dataSource, c, interceptors)
        }

        // No active consumers at all:
        // 1) All shards are migrating or
        // 2) The entire shard table contains references to invalid consumer nodes
        if (consumer == null) {
            return emptyList()
        }

        return consumer.receive(limit, receiveOptions)
    }

    override fun delete(messageIds: List<Id>): Int {
        val latestState = cluster.getState()

        val byNodes = latestState.mapShardsToNodes(messageIds) { it.shard }

        var deleted = byNodes
            .map { (node, ids) ->
                if (node == null) {
                    return@map 0
                }

                val consumer = latestState.getConsumer(this, node) { dataSource ->
                    DatabaseConsumer(dataSource, queue, consumerOptions, interceptors)
                }

                consumer.delete(ids)
            }.sum()

        if (deleted < messageIds.size) {
            val consumers = latestState.getConsumers(this) { dataSource: DataSource ->
                DatabaseConsumer(dataSource, queue, consumerOptions, interceptors)
            }

            consumers.forEach { consumer ->
                deleted += consumer.delete(messageIds)
            }
        }

        return deleted
    }
}
