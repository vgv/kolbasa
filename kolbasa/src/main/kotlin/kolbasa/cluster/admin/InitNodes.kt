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
    val tables = ClusterHelpers.readNodeTables(dataSources)
    ClusterHelpers.throwIfIdsMismatched(tables)

    val newNodes = mutableListOf<NewNode>()
    for ((nodeId, bucket) in bucketsOfNewNodes(dataSources, tables)) {
        newNodes += NewNode(nodeId, bucket, ClusterHelpers.hasMessages(dataSources.getValue(nodeId)))
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
    val tables = ClusterHelpers.readNodeTables(dataSources)
    // A wrong id means the node list is wrong, and a wrong list must not set up anything
    ClusterHelpers.throwIfIdsMismatched(tables)

    val buckets = bucketsOfNewNodes(dataSources, tables)
    val initialized = mutableListOf<NodeId>()
    for ((nodeId, bucket) in buckets) {
        if (IdSchema.createAndInitIdTable(dataSources.getValue(nodeId), nodeId, bucket)) {
            initialized += nodeId
        }
    }

    // Read the new nodes again. Another process may have set one up at the same moment, or two ids may point at the
    // same database
    val newNodes = dataSources.filterKeys { nodeId -> nodeId in buckets }
    ClusterHelpers.throwIfIdsMismatched(ClusterHelpers.readNodeTables(newNodes))

    return initialized
}

/**
 * The bucket of every node without an id: the lowest one that no node uses and that no queue table of any node holds an
 * id from. The nodes go in the order of their ids, so the same node list and the same data always get the same buckets
 *
 * A removed node's messages live on in the other nodes, with their ids. A new node with its bucket would make the same
 * ids again, and a later move of such a message would lose it: the target keeps its own row, and the source deletes
 * the moved one.
 *
 * @throws ClusterOperationException.NoFreeBucketException if every bucket is taken
 */
private fun bucketsOfNewNodes(dataSources: Map<NodeId, DataSource>, tables: NodeTables): SortedMap<NodeId, Int> {
    val result = sortedMapOf<NodeId, Int>()
    if (tables.withoutTable.isEmpty()) {
        return result
    }

    val usedBuckets = tables.found.values.map { node -> node.identifiersBucket }.toMutableSet()
    // Read once: every bucket is looked up in the same tables
    val queueTables = dataSources.mapValues { (_, dataSource) -> ClusterHelpers.queueTables(dataSource) }

    var bucket = Node.MIN_BUCKET
    for (nodeId in tables.withoutTable.sorted()) {
        while (bucket in usedBuckets || hasIdsOf(bucket, dataSources, queueTables)) {
            bucket++
            if (bucket > Node.MAX_BUCKET) {
                throw ClusterOperationException.NoFreeBucketException()
            }
        }
        usedBuckets += bucket
        result[nodeId] = bucket
    }
    return result
}

// Whether a queue table of any node holds an id from [bucket]
private fun hasIdsOf(
    bucket: Int,
    dataSources: Map<NodeId, DataSource>,
    queueTables: Map<NodeId, List<String>>
): Boolean {
    for ((nodeId, dataSource) in dataSources) {
        if (ClusterHelpers.hasIdsOf(bucket, dataSource, queueTables.getValue(nodeId))) {
            return true
        }
    }
    return false
}
