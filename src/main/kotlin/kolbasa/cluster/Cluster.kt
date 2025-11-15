package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.cluster.schema.ShardSchema
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.*
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

class Cluster @JvmOverloads constructor(
    private val dataSources: () -> List<DataSource>,
    private val queues: List<Queue<*>> = emptyList(),
) {

    @JvmOverloads
    constructor(dataSources: List<DataSource>, queues: List<Queue<*>> = emptyList()) : this({ dataSources }, queues)

    @Volatile
    private var state: ClusterState = ClusterState.NOT_INITIALIZED

    private var schemaUpdated: Boolean = false

    fun initAndScheduleStateUpdate() {
        updateStateOnce()

        val (interval, executor) = Kolbasa.clusterStateUpdateConfig

        executor.schedule(
            { initAndScheduleStateUpdate() },
            interval.seconds,
            TimeUnit.SECONDS
        )
    }

    @Synchronized
    fun updateStateOnce() {
        val dataSources = dataSources()
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
            SchemaHelpers.updateDatabaseSchema(dataSource, queues)
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
