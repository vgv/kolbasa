package kolbasa.cluster.admin

import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * See [ClusterAdmin.syncShards]. Returns how many rows of the shard table have changed on every node
 */
internal fun syncShards(dataSources: Map<NodeId, DataSource>): Map<NodeId, Int> {
    val nodes = ClusterHelpers.readNodes(dataSources)
    val copiesBefore = ClusterHelpers.readShards(nodes)
    // Nothing to copy. Which node gets a shard is a decision, and that is what initShards is for
    if (copiesBefore.mergedShards.isEmpty()) {
        throw ClusterOperationException.ShardsNotInitializedException()
    }

    ClusterHelpers.createShardTables(nodes)
    // Read again: a node that had no table has one now, and the repair needs the copy of every node
    val shardMap = ClusterHelpers.readShards(nodes)
    ClusterHelpers.repairShards(nodes, shardMap)

    val copiesAfter = ClusterHelpers.readShards(nodes).copies
    val changedRows = sortedMapOf<NodeId, Int>()
    for (node in nodes.keys) {
        val before = copiesBefore.copies[node] ?: emptyMap()
        val after = copiesAfter[node] ?: emptyMap()
        changedRows[node.id] = after.values.count { shard -> before[shard.shard] != shard }
    }

    return changedRows
}
