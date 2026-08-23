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

/**
 * A [Consumer] that receives messages from a cluster of nodes.
 *
 * Messages of one queue live on many nodes, and one `receive()` call reads from one node: the consumer picks a node
 * that currently owns shards for reading and takes messages from it. Different calls may pick different nodes, so a
 * normal receive loop visits the whole cluster over time. This also means the order of messages across the cluster
 * is not defined – only the order inside one node is.
 *
 * An empty result therefore does not always mean "the queue is empty". It also happens when the chosen node has
 * nothing ready, and when no node is available for reading at all, which is the case while every shard is being
 * migrated. Treat an empty list as "nothing for me right now" and call again.
 *
 * This class is a thin wrapper around a [Cluster]: it holds no connections of its own and creates a plain
 * [DatabaseConsumer][kolbasa.consumer.datasource.DatabaseConsumer] per node behind the scenes. Creating one is
 * cheap, but `consumerOptions` is fixed at that moment, so keep one instance per set of defaults you need. The
 * [Cluster] must be initialized (see [Cluster.initAndScheduleStateUpdate]) before the first `receive()`.
 *
 * This class implements the [Consumer] interface, so, it's easy to use instad of [DatabaseConsumer] if you need to
 * migrate from a single-node setup to a Kolbasa cluster.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val consumer: Consumer = ClusterConsumer(cluster)
 *
 * val messages = consumer.receive(orders, 100)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Consumer consumer = new ClusterConsumer(cluster);
 *
 * var messages = consumer.receive(orders, 100);
 * ```
 *
 * @see Cluster
 * @see kolbasa.consumer.datasource.DatabaseConsumer
 */
class ClusterConsumer @JvmOverloads constructor(
    private val cluster: Cluster,
    private val consumerOptions: ConsumerOptions = ConsumerOptions.DEFAULT
) : Consumer {

    override fun <Data> receive(queue: Queue<Data>, limit: Int, receiveOptions: ReceiveOptions): List<Message<Data>> {
        val latestState = cluster.getState()

        val consumer = latestState.getActiveConsumer(this) { nodeId, dataSource, shards ->
            val peer = ConnectionAwareDatabaseConsumer(nodeId, consumerOptions, shards)
            DatabaseConsumer(nodeId, dataSource, peer)
        }

        // No active consumers at all:
        // 1) All shards are migrating or
        // 2) The entire shard table contains references to invalid consumer nodes
        if (consumer == null) {
            return emptyList()
        }

        return consumer.receive(queue, limit, receiveOptions)

    }

    override fun <Data> delete(queue: Queue<Data>, messageIds: List<Id>): Int {
        val latestState = cluster.getState()

        val byNodes = latestState.mapShardsToNodes(messageIds) { it.shard }

        var deleted = byNodes
            .map { (node, ids) ->
                if (node == null) {
                    return@map 0
                }

                val consumer = latestState.getConsumer(this, node) { nodeId, dataSource ->
                    val peer = ConnectionAwareDatabaseConsumer(nodeId, consumerOptions, Shards.ALL_SHARDS)
                    DatabaseConsumer(nodeId, dataSource, peer)
                }

                consumer.delete(queue, ids)
            }.sum()

        if (deleted < messageIds.size) {
            val consumers = latestState.getConsumers(this) { nodeId, dataSource: DataSource ->
                val peer = ConnectionAwareDatabaseConsumer(nodeId, consumerOptions, Shards.ALL_SHARDS)
                DatabaseConsumer(nodeId, dataSource, peer)
            }

            consumers.forEach { consumer ->
                deleted += consumer.delete(queue, messageIds)
            }
        }

        return deleted
    }

}
