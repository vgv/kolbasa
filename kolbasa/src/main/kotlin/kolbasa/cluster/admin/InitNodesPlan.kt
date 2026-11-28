package kolbasa.cluster.admin

import kolbasa.schema.NodeId

/**
 * A node that [ClusterAdmin.initNodes] would give an id
 *
 * @property id the id it gets: the one it is listed under
 * @property bucket the bucket it gets. The bucket is the top bits of every message id the node creates, so no two nodes
 * share one
 * @property hasMessages whether a queue table of this node has a row, a dead letter queue or an archive among them. Such
 * a node was used without a cluster
 */
data class NewNode(
    val id: NodeId,
    val bucket: Int,
    val hasMessages: Boolean,
)

/**
 * What [ClusterAdmin.initNodes] would do: which nodes get an id, and with which bucket
 *
 * @property newNodes the nodes that have no id yet, in the order of their ids
 * @property initializedNodes the nodes that have their ids already. They stay as they are
 * @property unsafeNodes the new nodes that hold messages, in a cluster of more than one node. Alone, such a node becomes
 * a cluster of one node safely. With other nodes, its message ids can collide with theirs, and its messages of shards
 * that go to other nodes stay unread
 */
data class InitNodesPlan(
    val newNodes: List<NewNode>,
    val initializedNodes: List<NodeId>,
    val unsafeNodes: List<NodeId>,
) {

    /**
     * True when every node has its id already, so [ClusterAdmin.initNodes] has nothing to do
     */
    val isEmpty: Boolean = newNodes.isEmpty()
}
