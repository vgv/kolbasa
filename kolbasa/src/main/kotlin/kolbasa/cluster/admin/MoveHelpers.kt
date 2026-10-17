package kolbasa.cluster.admin

import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.readString
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

internal data class ShardInfo(
    val shardDataSource: DataSource,
    val shards: Map<ShardId, Shard>
)

internal data class SourceAndTargetNodes(
    val sourceNodes: List<DataSource>,
    val targetNode: DataSource
)

internal object MoveHelpers {

    /** A cause chain is usually two or three deep; a longer one is noise in an error message */
    private const val MAX_REPORTED_CAUSES = 3

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
            if (shards.size == ShardId.SHARD_COUNT) {
                return ShardInfo(dataSource, shards)
            }
        }

        throw ClusterOperationException.ExecutionException("Initialized shard table ${ShardSchema.SHARD_TABLE_NAME} not found. Is it a Kolbasa cluster?")
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

    fun calculateShardsDiff(initialShards: Map<ShardId, Shard>, updatedShards: Map<ShardId, Shard>): List<ShardDiff> {
        val difference = mutableListOf<ShardDiff>()

        initialShards.forEach { (shardId, initialShard) ->
            val updatedShard = updatedShards[shardId] ?: throw IllegalStateException("Can't find updated shard ${shardId.id}")

            if (initialShard != updatedShard) {
                difference += ShardDiff(initialShard.toOwnership(), updatedShard.toOwnership())
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

    /** Every distinct message down the cause chain, nearest first */
    private fun Throwable.causeMessages(): List<String> {
        val messages = linkedSetOf<String>()

        var current = cause
        while (current != null && messages.size < MAX_REPORTED_CAUSES) {
            current.message?.let(messages::add)
            current = current.cause
        }

        return messages.toList() - listOfNotNull(message)
    }

    fun checkClusterNodes(dataSources: List<DataSource>) {
        val errors = checkConnection(dataSources)
        if (errors.isEmpty()) {
            return
        }

        val errorMessage = buildString {
            appendLine("Can't connect to ${errors.size} of ${dataSources.size} kolbasa cluster nodes:")

            errors.forEach { (dataSource, error) ->
                appendLine("  Node: $dataSource")
                appendLine("  Error: ${error.message}")
                // A pooled DataSource reports a timeout of its own and keeps the real reason as the cause,
                // so without this an operator reads "request timed out" instead of "connection refused"
                error.causeMessages().forEach { cause ->
                    appendLine("  Caused by: $cause")
                }
                appendLine()
            }
        }

        throw ClusterOperationException.ExecutionException(errorMessage)
    }
}
