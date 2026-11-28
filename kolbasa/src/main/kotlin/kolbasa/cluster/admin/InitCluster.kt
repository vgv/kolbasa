package kolbasa.cluster.admin

import kolbasa.cluster.admin.check.ShardMapState
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * See [ClusterAdmin.planInitCluster]
 */
internal fun planInitCluster(dataSources: Map<NodeId, DataSource>): InitClusterPlan {
    val nodesPlan = planInitNodes(dataSources)

    // The nodes as they will be once the new ones have their ids: the id each is listed under, and the bucket of the
    // plan. A wrong id has stopped the plan above, so every node is either found or new
    val tables = MoveHelpers.readNodeTables(dataSources)
    val nodes = sortedMapOf<Node, DataSource>()
    for ((nodeId, node) in tables.found) {
        nodes[node] = dataSources.getValue(nodeId)
    }
    for (newNode in nodesPlan.newNodes) {
        nodes[Node(newNode.id, newNode.bucket)] = dataSources.getValue(newNode.id)
    }

    val shardMap = MoveHelpers.readShards(nodes)
    val shardsPlan = planShards(nodes, shardMap, target = null, shards = null)

    // The rows of the new shards go to every copy, so only the rows there are now can differ
    val copiesToSync = if (shardMap.mergedShards.isEmpty()) {
        0
    } else {
        val shardMapState = ShardMapState(
            nodes = nodes.keys.map { it.id }.toSet(),
            mergedShards = shardMap.mergedShards,
            copies = shardMap.copies.mapKeys { (node, _) -> node.id },
            invalidShards = emptyMap()
        )
        shardMapState.compute().differentCopies
    }

    return InitClusterPlan(nodesPlan, shardsPlan, copiesToSync)
}

/**
 * See [ClusterAdmin.initCluster]
 */
internal fun initCluster(dataSources: Map<NodeId, DataSource>): InitClusterResult {
    val initializedNodes = initNodes(dataSources)
    val owners = initShards(dataSources, target = null, shards = null)
    val changedRows = syncShards(dataSources)

    return InitClusterResult(initializedNodes, owners, changedRows)
}
