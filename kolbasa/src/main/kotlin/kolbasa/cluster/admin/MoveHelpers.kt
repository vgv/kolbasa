package kolbasa.cluster.admin

import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardCopies
import kolbasa.cluster.schema.ShardMap
import kolbasa.producer.ShardId
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaExtractor
import kolbasa.utils.JdbcHelpers.readBoolean
import kolbasa.utils.JdbcHelpers.readString
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

internal data class SourceAndTargetNodes(
    val sourceNodes: List<DataSource>,
    val targetNode: DataSource
)

/**
 * What `q__node` holds on every node, by the id the node is listed as
 */
internal class NodeTables(
    /**
     * The nodes whose `q__node` has its row, with what the row says
     */
    val found: Map<NodeId, Node>,

    /**
     * The nodes whose `q__node` has no active row. Such a node is broken, it is not a new one
     */
    val withEmptyTable: List<NodeId>,

    /**
     * The nodes without `q__node`. They are new and have no id yet
     */
    val withoutTable: List<NodeId>
)

internal object MoveHelpers {

    /**
     * A cause chain is usually two or three deep; a longer one is noise in an error message
     */
    private const val MAX_REPORTED_CAUSES = 3

    /**
     * Reads every node of [nodes] and checks that it is the node it is listed as. Changes nothing
     *
     * A node without an id is not given one. That is what sets it apart from
     * [ClusterHelper.readNodes][kolbasa.cluster.ClusterHelper.readNodes], which a [Cluster][kolbasa.cluster.Cluster] uses.
     *
     * @throws ClusterOperationException.NodeIdMismatchException if a node has another id, or a `q__node` table
     * without its row
     * @throws ClusterOperationException.NodesNotInitializedException if a node has no `q__node` table
     */
    fun readNodes(nodes: Map<NodeId, DataSource>): SortedMap<Node, DataSource> {
        val tables = readNodeTables(nodes)

        // A wrong id means a wrong node list, and an empty q__node a broken node. Either way it doesn't matter
        // which nodes have no id yet
        throwIfIdsMismatched(tables)
        if (tables.withoutTable.isNotEmpty()) {
            throw ClusterOperationException.NodesNotInitializedException(tables.withoutTable)
        }

        val result = sortedMapOf<Node, DataSource>()
        for ((nodeId, node) in tables.found) {
            result[node] = nodes.getValue(nodeId)
        }
        return result
    }

    /**
     * Reads `q__node` on every node of [nodes]. Changes nothing and checks nothing, see [throwIfIdsMismatched]
     */
    fun readNodeTables(nodes: Map<NodeId, DataSource>): NodeTables {
        val found = mutableMapOf<NodeId, Node>()
        val withEmptyTable = mutableListOf<NodeId>()
        val withoutTable = mutableListOf<NodeId>()

        for ((nodeId, dataSource) in nodes) {
            if (!IdSchema.nodeTableExists(dataSource)) {
                withoutTable += nodeId
                continue
            }

            val node = IdSchema.findNodeInfo(dataSource)
            if (node != null) {
                found[nodeId] = node
            } else {
                withEmptyTable += nodeId
            }
        }

        return NodeTables(found, withEmptyTable, withoutTable)
    }

    /**
     * Throws if a node has another id than the one it is listed as, or `q__node` without its row. The nodes without
     * `q__node` are not checked: they have no id yet
     */
    fun throwIfIdsMismatched(tables: NodeTables) {
        val mismatches = tables.found
            .mapNotNull { (expected, node) ->
                if (node.id != expected)
                    expected to node.id
                else
                    null
            }
            .toMap()

        if (mismatches.isNotEmpty() || tables.withEmptyTable.isNotEmpty()) {
            throw ClusterOperationException.NodeIdMismatchException(mismatches, tables.withEmptyTable)
        }
    }

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
     * Creates the shard table on every node that has none, see [ShardCopies.createTables]
     */
    fun createShardTables(nodes: SortedMap<Node, DataSource>) {
        reportFailure {
            ShardCopies.createTables(nodes)
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
     * Reads the shard map from every node, and changes nothing. Throws if there is no map, or it is not complete: every
     * node must have a shard table, and every shard must have a row. Copies may differ: the newest rows are the right ones
     */
    fun readCompleteShards(nodes: SortedMap<Node, DataSource>): Map<ShardId, Shard> {
        val shardMap = readShards(nodes)
        // No row anywhere is a cluster nobody has set up, not a map with gaps
        if (shardMap.mergedShards.isEmpty()) {
            throw ClusterOperationException.ShardsNotInitializedException()
        }

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
        reportPartialFailure(nodes) {
            ShardCopies.write(nodes, newRows)
        }
    }

    /**
     * Gives the shards of [owners] their nodes on every node, see [ShardCopies.fill]. Every node must have a shard table.
     *
     * Fails the same way as [writeShards]
     */
    fun fillShards(nodes: SortedMap<Node, DataSource>, owners: Map<ShardId, NodeId>) {
        reportPartialFailure(nodes) {
            ShardCopies.fill(nodes, owners)
        }
    }

    /**
     * Whether a queue table of this node has a row: a main queue, a dead letter queue or an archive
     */
    fun hasMessages(dataSource: DataSource): Boolean {
        val queueTables = SchemaExtractor.extractRawSchema(dataSource).values.filter { it.isQueueTable() }

        return queueTables.any { table -> dataSource.readBoolean("select exists(select 1 from ${table.name})") }
    }

    // A write to every node: one that fails doesn't stop the others, and the change is in effect once one node has it
    private fun reportPartialFailure(nodes: SortedMap<Node, DataSource>, block: () -> Unit) {
        try {
            block()
        } catch (e: Exception) {
            val failedNodes = 1 + e.suppressed.size
            val outcome = if (failedNodes < nodes.size) {
                "The other nodes have the change, so it is in effect. Once the failed nodes are back, sync the shards, " +
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
     * Checks that every node of [nodes] can be reached: one `select 1` on each, all at once. Changes nothing
     *
     * @throws ClusterOperationException.NodesUnreachableException naming every node that can't be reached, with what
     * the connection said
     */
    fun checkReachable(nodes: Map<NodeId, DataSource>) {
        val errors = ConcurrentHashMap<NodeId, Exception>()

        nodes.entries.parallelStream().forEach { (nodeId, dataSource) ->
            try {
                // Any fast query will do: the point is to open a connection
                dataSource.readString("select 1")
            } catch (e: Exception) {
                errors[nodeId] = e
            }
        }

        if (errors.isEmpty()) {
            return
        }

        val unreachable = errors.keys.sorted()
        val message = buildString {
            appendLine("Can't connect to ${unreachable.size} of ${nodes.size} cluster nodes:")
            for (nodeId in unreachable) {
                val error = errors.getValue(nodeId)
                appendLine("  ${nodeId.id}: ${error.message ?: error}")
                // A pool reports a timeout of its own and keeps the real reason as the cause, so without this an
                // operator reads "request timed out" instead of "connection refused"
                error.causeMessages().forEach { cause ->
                    appendLine("    Caused by: $cause")
                }
            }
        }

        throw ClusterOperationException.NodesUnreachableException(unreachable, message.trimEnd())
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
}
