package kolbasa.cluster.admin

import kolbasa.cluster.schema.ShardMap
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import java.util.*
import javax.sql.DataSource

/**
 * See [ClusterAdmin.planInitShards]. [shards] needs a [target]
 */
internal fun planInitShards(
    dataSources: Map<NodeId, DataSource>,
    target: NodeId?,
    shards: List<ShardId>?
): InitShardsPlan {
    val nodes = MoveHelpers.readNodes(dataSources)
    return planShards(nodes, MoveHelpers.readShards(nodes), target, shards)
}

/**
 * The plan of [planInitShards] for [nodes] and their [shardMap]. [ClusterAdmin.planInitCluster] makes it for the nodes
 * as they will be once they have their ids
 */
internal fun planShards(
    nodes: SortedMap<Node, DataSource>,
    shardMap: ShardMap,
    target: NodeId?,
    shards: List<ShardId>?
): InitShardsPlan {
    val owners = newOwners(nodes, shardMap, target, shards)

    // Only a new map decides where the messages already on the nodes belong. In a working cluster that has lost some
    // rows every node holds messages, and that is normal. The target gets the shards of its messages, so it is safe
    val unsafeNodes = mutableListOf<NodeId>()
    if (nodes.size > 1 && shardMap.mergedShards.isEmpty()) {
        for ((node, dataSource) in nodes) {
            if (node.id != target && MoveHelpers.hasMessages(dataSource)) {
                unsafeNodes += node.id
            }
        }
    }

    return InitShardsPlan(owners, unsafeNodes)
}

/**
 * See [ClusterAdmin.initShards]. [shards] needs a [target]
 */
internal fun initShards(
    dataSources: Map<NodeId, DataSource>,
    target: NodeId?,
    shards: List<ShardId>?
): Map<ShardId, NodeId> {
    val nodes = MoveHelpers.readNodes(dataSources)
    val owners = newOwners(nodes, MoveHelpers.readShards(nodes), target, shards)
    if (owners.isEmpty()) {
        return owners
    }

    // Every node gets the new rows, so the map is complete as soon as this is done. A shard that has a row on a node
    // already keeps it there
    MoveHelpers.createShardTables(nodes)
    MoveHelpers.fillShards(nodes, owners)

    // Read back: an application may have given some of the shards a node at the same moment, and its row may have won
    val mergedShards = MoveHelpers.readShards(nodes).mergedShards
    val result = sortedMapOf<ShardId, NodeId>()
    for (shard in owners.keys) {
        result[shard] = mergedShards.getValue(shard).producerNode
    }
    return result
}

/**
 * The new node of every shard that has no row on any node: spread evenly over the nodes, see [ShardSchema.spreadEvenly],
 * or all of them to [target]. With [shards], only those shards go to [target], and each of them must have no row
 */
private fun newOwners(
    nodes: SortedMap<Node, DataSource>,
    shardMap: ShardMap,
    target: NodeId?,
    shards: List<ShardId>?
): SortedMap<ShardId, NodeId> {
    if (target == null) {
        return ShardSchema.spreadEvenly(nodes.keys.map { it.id }, shardMap.mergedShards.values, shardMap.shardsWithoutRow())
    }

    if (nodes.keys.none { node -> node.id == target }) {
        throw ClusterOperationException.MoveToNonExistingNodeException(nodes.keys.map { it.id }, target)
    }

    val shardsToFill = if (shards == null) {
        shardMap.shardsWithoutRow()
    } else {
        // A shard with a row has a node, and changing it is a migration. A new row could even win over the old one: on
        // a copy that lacks the old row, with the same initial revision
        val initialized = shards.distinct().filter { shard -> shard in shardMap.mergedShards }
        if (initialized.isNotEmpty()) {
            throw ClusterOperationException.ShardsAlreadyInitializedException(initialized)
        }
        shards.distinct()
    }

    val result = sortedMapOf<ShardId, NodeId>()
    for (shard in shardsToFill) {
        result[shard] = target
    }
    return result
}
