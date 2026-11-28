package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * What [ClusterAdmin.initShards] would do: which node every shard without a row gets
 *
 * @property owners the node of every shard that gets one, in the order of the shards: every shard that has no row on any
 * node, or only the shards asked for. Without a target, a shard goes to the node that has the fewest shards at that
 * moment, so on a new cluster the nodes take turns
 * @property unsafeNodes the nodes that hold messages, when the cluster gets its first shard map and has more than one
 * node. The messages of such a node whose shards go to other nodes stay unread. The target is never among them: it
 * gets the shards of its own messages
 */
data class InitShardsPlan(
    val owners: Map<ShardId, NodeId>,
    val unsafeNodes: List<NodeId>,
) {

    /**
     * True when every shard has a row on some node already, so [ClusterAdmin.initShards] has nothing to do
     */
    val isEmpty: Boolean = owners.isEmpty()
}
