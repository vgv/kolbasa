package kolbasa.cluster.migrate.utils

import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.schema.Node
import java.util.*
import javax.sql.DataSource

internal data class ShardInfo(
    val shardDataSource: DataSource,
    val shards: Map<Int, Shard>
)

internal data class SourceAndTargetNodes(
    val sourceNodes: List<DataSource>,
    val targetNode: DataSource
)

internal data class ShardDiff(
    val originalShard: Shard,
    val updatedShard: Shard
)

internal object MigrateHelpers {

    fun readShards(nodes: SortedMap<Node, DataSource>): ShardInfo {
        // Try to find a node with a 100% initialized shard table, if found, return it
        for ((_, dataSource) in nodes) {
            val shards = try {
                ShardSchema.readShards(dataSource)
            } catch (_: Exception) {
                // Shard table doesn't exist
                emptyMap()
            }

            // Shard table is 100% initialized, return it
            if (shards.size == Shard.SHARD_COUNT) {
                return ShardInfo(dataSource, shards)
            }
        }

        throw IllegalStateException("Initialized shard table not found")
    }

    fun splitNodes(nodes: SortedMap<Node, DataSource>, targetNodeId: String): SourceAndTargetNodes {
        val sourceNodes = mutableListOf<DataSource>()
        var targetNode: DataSource? = null
        for ((node, dataSource) in nodes) {
            if (node.serverId == targetNodeId) {
                targetNode = dataSource
            } else {
                sourceNodes += dataSource
            }
        }

        check(sourceNodes.size == nodes.size - 1 && targetNode != null) {
            "Invalid state"
        }

        return SourceAndTargetNodes(sourceNodes, targetNode)
    }

    fun calculateShardsDiff(initialShards: Map<Int, Shard>, updatedShards: Map<Int, Shard>): List<ShardDiff> {
        val difference = mutableListOf<ShardDiff>()

        initialShards.forEach { (shardNumber, initialShard) ->
            val updatedShard =
                updatedShards[shardNumber] ?: throw IllegalStateException("Can't find updated shard $shardNumber")
            if (initialShard != updatedShard) {
                difference += ShardDiff(initialShard, updatedShard)
            }
        }

        return difference
    }
}
