package kolbasa.cluster.butcher

import kolbasa.cluster.Shard
import kolbasa.cluster.butcher.config.ClusterNodes
import kolbasa.cluster.schema.ShardSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.readString
import java.util.*
import java.util.concurrent.ConcurrentHashMap
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

internal object MoveHelpers {

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

        throw ButcherException.ExecutionException("Initialized shard table ${ShardSchema.SHARD_TABLE_NAME} not found. Is it a Kolbasa cluster?")
    }

    fun splitNodes(nodes: SortedMap<Node, DataSource>, targetNodeId: NodeId): SourceAndTargetNodes {
        val sourceNodes = mutableListOf<DataSource>()
        var targetNode: DataSource? = null
        for ((node, dataSource) in nodes) {
            if (node.id == targetNodeId) {
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
            val updatedShard = updatedShards[shardNumber] ?: throw IllegalStateException("Can't find updated shard $shardNumber")

            if (initialShard != updatedShard) {
                difference += ShardDiff(initialShard, updatedShard)
            }
        }

        return difference
    }

    /**
     * Validate connectivity to all nodes.
     */
    fun checkConnection(dataSources: List<DataSource>): Map<DataSource, Exception> {
        val errors = ConcurrentHashMap<DataSource, Exception>()

        dataSources.parallelStream().forEach { dataSource ->
            try {
                // Any fast query will do — we only need to confirm we can open a connection
                dataSource.readString("select 1")
            } catch (e: Exception) {
                errors[dataSource] = e
            }
        }

        return errors
    }

    fun checkClusterNodes(nodes: ClusterNodes) {
        val errors = checkConnection(nodes.dataSources)
        if (errors.isEmpty()) {
            return
        }

        val errorMessage = buildString {
            appendLine("Can't connect to ${errors.size} of ${nodes.dataSources.size} kolbasa cluster nodes:")

            errors.forEach { (dataSource, error) ->
                appendLine("  Node: $dataSource")
                appendLine("  Error: ${error.message}")
                appendLine()
            }
        }

        throw ButcherException.ExecutionException(errorMessage)
    }
}
