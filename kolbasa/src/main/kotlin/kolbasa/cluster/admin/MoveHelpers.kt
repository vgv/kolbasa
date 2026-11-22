package kolbasa.cluster.admin

import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardCopies
import kolbasa.cluster.schema.ShardMap
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.readString
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

internal data class SourceAndTargetNodes(
    val sourceNodes: List<DataSource>,
    val targetNode: DataSource
)

internal object MoveHelpers {

    /**
     * A cause chain is usually two or three deep; a longer one is noise in an error message
     */
    private const val MAX_REPORTED_CAUSES = 3

    /**
     * Reads the shard map from every node and changes nothing, see [ShardCopies.readOnly]
     */
    fun readShards(nodes: SortedMap<Node, DataSource>): ShardMap {
        return reportFailure {
            ShardCopies.readOnly(nodes)
        }
    }

    /**
     * Reads the shard map the same way an application does, see [ShardCopies.read]. It creates what is missing
     */
    fun readOrCreateShards(nodes: SortedMap<Node, DataSource>): ShardMap {
        return reportFailure {
            ShardCopies.read(nodes, knownShards = emptyMap(), knownFingerprint = null)
        }
    }

    /**
     * Puts [newRows] into the shard map, on every node. The copies that lag behind catch up on the way.
     *
     * A node that can't take the rows doesn't stop the others, and the change is in effect once one node has it. Then
     * this method throws, with the name of the failed node. It is safe to run the operation again
     */
    fun writeShards(nodes: SortedMap<Node, DataSource>, shardMap: ShardMap, newRows: List<Shard>) {
        val mergedShards = shardMap.mergedShards + newRows.associateBy { it.shard }

        reportFailure {
            ShardCopies.repair(nodes, ShardMap(mergedShards, shardMap.copies))
        }
    }

    private fun <T> reportFailure(block: () -> T): T {
        try {
            return block()
        } catch (e: Exception) {
            val errorMessage = buildString {
                // Every failed node: the first one, and the suppressed ones
                (listOf<Throwable>(e) + e.suppressed).forEach { failure ->
                    appendLine(failure.message ?: failure.toString())
                    failure.causeMessages().forEach { cause ->
                        appendLine("  Caused by: $cause")
                    }
                }
            }
            throw ClusterOperationException.ExecutionException(errorMessage)
        }
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

            // A row whose revision alone has changed is not a difference
            if (!initialShard.equalsIgnoreRevision(updatedShard)) {
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

    /**
     * Every distinct message down the cause chain, nearest first
     */
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
