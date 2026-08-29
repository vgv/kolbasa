package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.cluster.schema.ShardSchema
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import javax.sql.DataSource

/**
 * One kolbasa cluster – the set of PostgreSQL nodes your queues are spread over.
 *
 * In a cluster every queue is split into 1024 shards, and each shard belongs to one node. The map of "which node
 * owns which shard" is kept in the database, not in your code, and it changes when shards are moved between nodes.
 * This object holds a [DataSource] for every node, keeps a local copy of that map, and refreshes it from time to
 * time. The cluster-aware roles – [ClusterProducer], [ClusterConsumer], [ClusterMutator] and [ClusterInspector] –
 * use it to find the right node for every call.
 *
 * Create one instance per cluster, at application startup, and share it. It is a long-lived object: it keeps
 * connections to all nodes and schedules a background task. Do not create it per request, per queue or per call.
 * The roles built on top of it are cheap, but they are best created once as well.
 *
 * A new instance knows nothing about the nodes yet, so call [initAndScheduleStateUpdate] before you use any role.
 * If you skip this step, every role call fails with `Cluster state isn't initialized`.
 *
 * Growing the cluster needs nothing from your code beyond the node list itself. Build the [Cluster] with a function
 * that returns the current nodes, and a node added to that list is picked up by the next state update: the queues
 * you passed in `queues` are created on it, and shards can then be migrated to it.
 *
 * ## Usage Example
 *
 * ```kotlin
 * // One DataSource per node
 * val cluster = Cluster(listOf(nodeA, nodeB, nodeC), listOf(orders, customers))
 *
 * // Read the shard map now, and keep it fresh in the background
 * cluster.initAndScheduleStateUpdate()
 *
 * val producer: Producer = ClusterProducer(cluster)
 * val consumer: Consumer = ClusterConsumer(cluster)
 *
 * // From here on nothing is cluster-specific – the same interfaces, the same calls
 * producer.send(orders, "message")
 * val messages = consumer.receive(orders, 100)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var cluster = new Cluster(List.of(nodeA, nodeB, nodeC), List.of(orders, customers));
 *
 * cluster.initAndScheduleStateUpdate();
 *
 * Producer producer = new ClusterProducer(cluster);
 * Consumer consumer = new ClusterConsumer(cluster);
 *
 * producer.send(orders, "message");
 * var messages = consumer.receive(orders, 100);
 * ```
 *
 * This KDoc covers the API only. The model behind it – how a shard is owned by a node, how ownership moves from one
 * node to another, what the internal `q__shard` table holds, and what to do when you add or remove a node – is
 * described in
 * [Cluster architecture](https://github.com/vgv/kolbasa/blob/main/docs/Cluster%20architecture.md).
 *
 * @constructor Creates a cluster whose node list is read from the `dataSources` function. The function is called
 * again on every state update, so use this form when nodes can be added or removed while the application runs.
 * @param dataSourcesProvider returns the [DataSource] of every node of this cluster. It must never return an empty list –
 * a state update over an empty list fails with `Data sources list is empty`.
 * @param queues queues to create or update on every node of the cluster. Kolbasa does this during a state update,
 * for the nodes that do not have them yet, so a node added later gets the whole schema on its own – within one
 * refresh interval, or at once if you call [updateStateOnce]. An empty list means "manage routing only, the schema
 * is mine": nothing here touches your tables, and creating them stays your job, with
 * [SchemaHelpers.createOrUpdateQueues][kolbasa.schema.SchemaHelpers.createOrUpdateQueues].
 */
class Cluster @JvmOverloads constructor(
    private val dataSourcesProvider: Supplier<List<DataSource>>,
    private val queues: List<Queue<*>> = emptyList(),
) {

    /**
     * Creates a cluster over a fixed list of nodes.
     *
     * Use this form when the node list is known at startup and does not change while the application runs. Adding a
     * node later means creating a new [Cluster] with the longer list.
     *
     * @param dataSources the [DataSource] of every node of this cluster, at least one
     * @param queues queues to create or update on every node, see the primary constructor for the details. With a
     * fixed node list there are no nodes added later, so this happens once, at the first state update.
     */
    @JvmOverloads
    constructor(dataSources: List<DataSource>, queues: List<Queue<*>> = emptyList()) : this({ dataSources }, queues)

    @Volatile
    private var state: ClusterState = ClusterState.NOT_INITIALIZED

    private var schemaUpdated: Boolean = false

    /**
     * Reads the cluster state now, and schedules the next read.
     *
     * Call this once, at application startup, right after the [Cluster] is created and before any role built on it
     * is used. It does two things: it runs [updateStateOnce] immediately, so the cluster is usable when the method
     * returns, and it asks the executor of
     * [Kolbasa.clusterStateUpdateConfig][kolbasa.Kolbasa.clusterStateUpdateConfig] to call it again later. Every run
     * schedules the next one, so the refresh continues for the life of the application. There is no way to stop it.
     *
     * The interval and the executor are read again before every reschedule, so a change to
     * [Kolbasa.clusterStateUpdateConfig][kolbasa.Kolbasa.clusterStateUpdateConfig] takes effect on the next cycle.
     *
     * Calling this method a second time on the same [Cluster] starts a second, independent chain of updates. There
     * is no harm in the extra work, but there is no benefit either.
     */
    fun initAndScheduleStateUpdate() {
        updateStateOnce()

        val (interval, executor) = Kolbasa.clusterStateUpdateConfig

        executor.schedule(
            { initAndScheduleStateUpdate() },
            interval.seconds,
            TimeUnit.SECONDS
        )
    }

    /**
     * Reads the cluster state once, right now, and returns when it is done.
     *
     * One call asks every node for its identity, reads the shard ownership map, and – if the [Cluster] was created
     * with a list of queues – creates or updates those queues on the nodes that do not have them yet. If no node
     * has a complete shard map yet, this method builds one and stores it, which is how a fresh cluster is
     * initialized.
     *
     * Most applications never call this directly: [initAndScheduleStateUpdate] calls it and keeps calling it. Use it
     * on its own when you want the new state at a known moment instead of waiting for the next scheduled run – after
     * shards were moved between nodes, for example, or in a test.
     *
     * The method is synchronized, so calls from several threads run one after another. It talks to every node, so it
     * is not cheap – do not call it per message.
     */
    @Synchronized
    fun updateStateOnce() {
        val dataSources = dataSourcesProvider.get()
        check(dataSources.isNotEmpty()) {
            "Data sources list is empty"
        }

        val nodes = initNodes(dataSources)
        val shards = initShards(nodes)
        initQueuesSchema(dataSources)

        val newState = ClusterState(nodes.mapKeys { it.key.id }, shards)
        if (newState != state) {
            state = newState
        }
    }

    private fun initNodes(dataSources: List<DataSource>): SortedMap<Node, DataSource> {
        var nodes = ClusterHelper.readNodes(dataSources)

        while (remapBucketIdentifiers(nodes)) {
            nodes = ClusterHelper.readNodes(dataSources)
        }

        return nodes
    }

    private fun remapBucketIdentifiers(nodes: SortedMap<Node, DataSource>): Boolean {
        val existingBuckets = nodes.keys.map { it.identifiersBucket }.toSet()
        val assignedBuckets = mutableSetOf<Int>()
        var nextBucket = Node.MIN_BUCKET
        var remappedAtLeastOnce = false

        nodes.forEach { (node, dataSource) ->
            val currentBucket = node.identifiersBucket

            if (currentBucket in assignedBuckets) {
                // if we are here, it means that the current bucket is already assigned to another node earlier
                // we need to remap it to the next available bucket
                while ((nextBucket in assignedBuckets) || (nextBucket in existingBuckets)) {
                    nextBucket++
                }

                IdSchema.updateIdentifiersBucket(dataSource, currentBucket, nextBucket)
                assignedBuckets += nextBucket
                remappedAtLeastOnce = true
            } else {
                assignedBuckets += currentBucket
            }
        }

        return remappedAtLeastOnce
    }

    private fun initShards(nodes: SortedMap<Node, DataSource>): Map<Int, Shard> {
        // First, try to find a node with a 100% initialized shard table, if found, return it
        for ((_, dataSource) in nodes) {
            val shards = try {
                ShardSchema.readShards(dataSource)
            } catch (_: Exception) {
                // Shard table doesn't exist
                emptyMap()
            }

            // Shard table is 100% initialized, return it
            if (shards.size == Shard.SHARD_COUNT) {
                return shards
            }
        }

        // No fully initialized shard table found, let's create a
        // fully initialized shard table on the node with the smallest id
        val (_, dataSource) = nodes.entries.first()
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())
        return ShardSchema.readShards(dataSource)
    }

    private fun initQueuesSchema(dataSources: List<DataSource>) {
        if (schemaUpdated || queues.isEmpty()) {
            return
        }

        dataSources.forEach { dataSource: DataSource ->
            SchemaHelpers.createOrUpdateQueues(dataSource, queues)
        }

        schemaUpdated = true
    }


    internal fun getState(): ClusterState {
        check(state !== ClusterState.NOT_INITIALIZED) {
            "Cluster state isn't initialized, maybe you forgot to call updateState() method?"
        }

        return state
    }

}
