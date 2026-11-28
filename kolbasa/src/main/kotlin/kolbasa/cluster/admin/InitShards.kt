package kolbasa.cluster.admin

import kolbasa.cluster.schema.ShardMap
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import java.util.*
import javax.sql.DataSource

/**
 * See [ClusterAdmin.planInitShards]
 */
internal fun planInitShards(dataSources: Map<NodeId, DataSource>): InitShardsPlan {
    val nodes = MoveHelpers.readNodes(dataSources)
    val shardMap = MoveHelpers.readShards(nodes)

    // Only a new map decides where the messages already on the nodes belong. In a working cluster that has lost some
    // rows every node holds messages, and that is normal
    val unsafeNodes = mutableListOf<NodeId>()
    if (nodes.size > 1 && shardMap.mergedShards.isEmpty()) {
        for ((node, dataSource) in nodes) {
            if (MoveHelpers.hasMessages(dataSource)) {
                unsafeNodes += node.id
            }
        }
    }

    return InitShardsPlan(ownersOfShardsWithoutRow(nodes, shardMap), unsafeNodes)
}

/**
 * See [ClusterAdmin.initShards]
 */
internal fun initShards(dataSources: Map<NodeId, DataSource>): Map<ShardId, NodeId> {
    val nodes = MoveHelpers.readNodes(dataSources)
    val owners = ownersOfShardsWithoutRow(nodes, MoveHelpers.readShards(nodes))
    if (owners.isEmpty()) {
        return owners
    }

    // Every node gets the new rows, so the map is complete as soon as this is done. A shard that has a row on a node
    // already keeps it there
    MoveHelpers.createShardTables(nodes)
    MoveHelpers.fillShards(nodes, owners)

    return owners
}

/**
 * The node of every shard that has no row on any node, see [ShardSchema.spreadEvenly]
 */
private fun ownersOfShardsWithoutRow(nodes: SortedMap<Node, DataSource>, shardMap: ShardMap): SortedMap<ShardId, NodeId> {
    return ShardSchema.spreadEvenly(nodes.keys.map { it.id }, shardMap.mergedShards.values, shardMap.shardsWithoutRow())
}
