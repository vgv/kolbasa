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
     * The rows of the shard tables with a shard number that doesn't exist, see [ShardCopies.readInvalidShards]
     */
    fun readInvalidShards(nodes: SortedMap<Node, DataSource>, shardMap: ShardMap): Map<Node, List<Int>> {
        return reportFailure {
            ShardCopies.readInvalidShards(nodes, shardMap)
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
     * Brings the copies on every node up to date, see [ShardCopies.repair]
     */
    fun repairShards(nodes: SortedMap<Node, DataSource>, shardMap: ShardMap) {
        reportFailure {
            ShardCopies.repair(nodes, shardMap)
        }
    }

    /**
     * Reads the shard map from every node, and changes nothing. Throws if the map is not complete: every node must have a
     * shard table, and every shard must have a row. Copies may differ: the newest rows are the right ones
     */
    fun readCompleteShards(nodes: SortedMap<Node, DataSource>): Map<ShardId, Shard> {
        val shardMap = readShards(nodes)

        val nodesWithoutTable = nodes.keys.filter { it !in shardMap.copies }.map { it.id }.toSet()
        val shardsWithoutRow = shardMap.shardsWithoutRow().toSet()
        if (nodesWithoutTable.isNotEmpty() || shardsWithoutRow.isNotEmpty()) {
            throw ClusterOperationException.IncompleteShardMapException(nodesWithoutTable, shardsWithoutRow)
        }

        return shardMap.mergedShards
    }

    /**
     * Writes [newRows] to every node, see [ShardCopies.write].
     *
     * A node that can't take the rows doesn't stop the others, and the change is in effect once one node has it. Then
     * this method throws, with the name of the failed node
     */
    fun writeShards(nodes: SortedMap<Node, DataSource>, newRows: List<Shard>) {
        try {
            ShardCopies.write(nodes, newRows)
        } catch (e: Exception) {
            val failedNodes = 1 + e.suppressed.size
            val outcome = if (failedNodes < nodes.size) {
                "The other nodes have the change, so it is in effect. Once the failed nodes are back, repair the shard map, " +
                    "or wait for an application to do it"
            } else {
                "No node has the change, so nothing has changed"
            }
            throw ClusterOperationException.ExecutionException(describeFailure(e) + outcome)
        }
    }

    private fun <T> reportFailure(block: () -> T): T {
        try {
            return block()
        } catch (e: Exception) {
            throw ClusterOperationException.ExecutionException(describeFailure(e))
        }
    }

    // Every failed node: the first one, and the suppressed ones
    private fun describeFailure(e: Exception): String {
        return buildString {
            (listOf<Throwable>(e) + e.suppressed).forEach { failure ->
                appendLine(failure.message ?: failure.toString())
                failure.causeMessages().forEach { cause ->
                    appendLine("  Caused by: $cause")
                }
            }
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
