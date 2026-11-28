package kolbasa.cluster.admin

import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Returns how many rows of the shard table have changed on every node
 */
internal fun createOrRepairShardMap(dataSources: Map<NodeId, DataSource>): Map<NodeId, Int> {
    val nodes = MoveHelpers.readNodes(dataSources)
    val copiesBefore = MoveHelpers.readShards(nodes).copies

    val shardMap = MoveHelpers.readOrCreateShards(nodes)
    MoveHelpers.repairShards(nodes, shardMap)

    val copiesAfter = MoveHelpers.readShards(nodes).copies
    val changedRows = sortedMapOf<NodeId, Int>()
    for (node in nodes.keys) {
        val before = copiesBefore[node] ?: emptyMap()
        val after = copiesAfter[node] ?: emptyMap()
        changedRows[node.id] = after.values.count { shard -> before[shard.shard] != shard }
    }

    return changedRows
}
