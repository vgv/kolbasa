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

class ClusterConsumer<Data, Meta : Any>(
    private val cluster: Cluster,
    private val queue: Queue<Data, Meta>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions()
) : Consumer<Data, Meta> {

    override fun <D, M : Any> receive(queue: Queue<D, M>, limit: Int, receiveOptions: ReceiveOptions<M>): List<Message<D, M>> {
        val latestState = cluster.getState()

        val consumer = latestState.getActiveConsumer(this) { dataSource, shards ->
            val c = ConnectionAwareDatabaseConsumer(consumerOptions, shards)
            DatabaseConsumer(dataSource, queue, c)
        }

        // No active consumers at all:
        // 1) All shards are migrating or
        // 2) The entire shard table contains references to invalid consumer nodes
        if (consumer == null) {
            return emptyList()
        }

        return consumer.receive(limit, receiveOptions)

    }

    override fun <D, M : Any> delete(queue: Queue<D, M>, messageIds: List<Id>): Int {
        val latestState = cluster.getState()

        val byNodes = latestState.mapShardsToNodes(messageIds) { it.shard }

        var deleted = byNodes
            .map { (node, ids) ->
                if (node == null) {
                    return@map 0
                }

                val consumer = latestState.getConsumer(this, node) { dataSource ->
                    DatabaseConsumer(dataSource, queue, consumerOptions)
                }

                consumer.delete(ids)
            }.sum()

        if (deleted < messageIds.size) {
            val consumers = latestState.getConsumers(this) { dataSource: DataSource ->
                DatabaseConsumer(dataSource, queue, consumerOptions)
            }

            consumers.forEach { consumer ->
                deleted += consumer.delete(messageIds)
            }
        }

        return deleted
    }


    // -----------------------------------------------------------------------------------------------------

    override fun receive(limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        return receive(queue, limit, receiveOptions)
    }

    override fun delete(messageIds: List<Id>): Int {
        return delete(queue, messageIds)
    }
}
