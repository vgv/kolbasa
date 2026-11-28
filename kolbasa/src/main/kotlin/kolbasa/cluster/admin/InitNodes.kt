package kolbasa.cluster.admin

import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import java.util.*
import javax.sql.DataSource

/**
 * See [ClusterAdmin.planInitNodes]
 */
internal fun planInitNodes(dataSources: Map<NodeId, DataSource>): InitNodesPlan {
    val tables = MoveHelpers.readNodeTables(dataSources)
    MoveHelpers.throwIfIdsMismatched(tables)

    val newNodes = mutableListOf<NewNode>()
    for ((nodeId, bucket) in bucketsOfNewNodes(tables)) {
        newNodes += NewNode(nodeId, bucket, MoveHelpers.hasMessages(dataSources.getValue(nodeId)))
    }

    // Alone, a node with messages is a standalone database that becomes a cluster, and that is safe
    val unsafeNodes = if (dataSources.size > 1) {
        newNodes.filter { node -> node.hasMessages }.map { node -> node.id }
    } else {
        emptyList()
    }

    return InitNodesPlan(newNodes, tables.found.keys.sorted(), unsafeNodes)
}

/**
 * See [ClusterAdmin.initNodes]
 */
internal fun initNodes(dataSources: Map<NodeId, DataSource>): List<NodeId> {
    val tables = MoveHelpers.readNodeTables(dataSources)
    // A wrong id means the node list is wrong, and a wrong list must not set up anything
    MoveHelpers.throwIfIdsMismatched(tables)

    val buckets = bucketsOfNewNodes(tables)
    val initialized = mutableListOf<NodeId>()
    for ((nodeId, bucket) in buckets) {
        if (IdSchema.createAndInitIdTable(dataSources.getValue(nodeId), nodeId, bucket)) {
            initialized += nodeId
        }
    }

    // Read the new nodes again. Another process may have set one up at the same moment, or two ids may point at the
    // same database
    val newNodes = dataSources.filterKeys { nodeId -> nodeId in buckets }
    MoveHelpers.throwIfIdsMismatched(MoveHelpers.readNodeTables(newNodes))

    return initialized
}

/**
 * The bucket of every node without an id: the lowest one no other node uses. The nodes go in the order of their ids,
 * so the same node list always gets the same buckets
 */
private fun bucketsOfNewNodes(tables: NodeTables): SortedMap<NodeId, Int> {
    val usedBuckets = tables.found.values.map { node -> node.identifiersBucket }.toMutableSet()

    val result = sortedMapOf<NodeId, Int>()
    for (nodeId in tables.withoutTable.sorted()) {
        val bucket = (Node.MIN_BUCKET..Node.MAX_BUCKET).first { bucket -> bucket !in usedBuckets }
        usedBuckets += bucket
        result[nodeId] = bucket
    }
    return result
}
