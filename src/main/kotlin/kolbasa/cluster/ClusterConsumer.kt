package kolbasa.cluster

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.consumer.datasource.Consumer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.Id
import kolbasa.queue.Queue
import javax.sql.DataSource

class ClusterConsumer(
    private val cluster: Cluster,
    private val consumerOptions: ConsumerOptions = ConsumerOptions()
) : Consumer {

    override fun <Data, Meta : Any> receive(queue: Queue<Data, Meta>, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        val latestState = cluster.getState()

        val consumer = latestState.getActiveConsumer(this) { dataSource, shards ->
            val c = ConnectionAwareDatabaseConsumer(consumerOptions, shards)
            DatabaseConsumer(dataSource, c)
        }

        // No active consumers at all:
        // 1) All shards are migrating or
        // 2) The entire shard table contains references to invalid consumer nodes
        if (consumer == null) {
            return emptyList()
        }

        return consumer.receive(queue, limit, receiveOptions)

    }

    override fun <Data, Meta : Any> delete(queue: Queue<Data, Meta>, messageIds: List<Id>): Int {
        val latestState = cluster.getState()

        val byNodes = latestState.mapShardsToNodes(messageIds) { it.shard }

        var deleted = byNodes
            .map { (node, ids) ->
                if (node == null) {
                    return@map 0
                }

                val consumer = latestState.getConsumer(this, node) { dataSource ->
                    DatabaseConsumer(dataSource, consumerOptions)
                }

                consumer.delete(queue, ids)
            }.sum()

        if (deleted < messageIds.size) {
            val consumers = latestState.getConsumers(this) { dataSource: DataSource ->
                DatabaseConsumer(dataSource, consumerOptions)
            }

            consumers.forEach { consumer ->
                deleted += consumer.delete(queue, messageIds)
            }
        }

        return deleted
    }

}
