package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * What [ClusterAdmin.initCluster] would do: which nodes get an id, which shards get a node, and how many copies of the
 * shard map get rows from the others
 *
 * @property nodes which nodes get an id, see [ClusterAdmin.planInitNodes]
 * @property shards which shards get a node, see [ClusterAdmin.planInitShards]. It is worked out for the nodes as they
 * will be once they have their ids
 * @property copiesToSync how many copies of the shard map lack rows the other copies have, or hold older ones. The rows
 * of the shards that get a node go to every copy, so on a new cluster this is zero
 */
data class InitClusterPlan(
    val nodes: InitNodesPlan,
    val shards: InitShardsPlan,
    val copiesToSync: Int,
) {

    /**
     * The unsafe nodes of [nodes] and of [shards], each once, in the order of their ids
     */
    val unsafeNodes: List<NodeId> = (nodes.unsafeNodes + shards.unsafeNodes).distinct().sorted()

    /**
     * True when the cluster is set up already, so [ClusterAdmin.initCluster] has nothing to do
     */
    val isEmpty: Boolean = nodes.isEmpty && shards.isEmpty && copiesToSync == 0
}

/**
 * What [ClusterAdmin.initCluster] did
 *
 * @property initializedNodes the nodes that got an id, see [ClusterAdmin.initNodes]
 * @property owners the node of every shard that got one, read back after the write, see [ClusterAdmin.initShards]
 * @property changedRows how many rows of the shard map changed on every node when the copies were synced, see
 * [ClusterAdmin.syncShards]
 */
data class InitClusterResult(
    val initializedNodes: List<NodeId>,
    val owners: Map<ShardId, NodeId>,
    val changedRows: Map<NodeId, Int>,
)
