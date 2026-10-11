package kolbasa.schema

/**
 * Identifier of one node – one PostgreSQL server – of a kolbasa cluster
 *
 * A node is given its id once, when its internal `q__node` table is first created, and keeps it forever. The
 * id is a short random string rather than a host name or a position in a configuration file, so it survives
 * moving the database, changing its address and reordering the node list of a [Cluster][kolbasa.cluster.Cluster].
 *
 * Node ids appear whenever a cluster is described from the outside: which node stores a shard, which node a
 * shard is migrating to, which node a report is about. A standalone, non-cluster deployment has exactly one
 * node and never needs to name it.
 *
 * See [Cluster architecture](https://github.com/vgv/kolbasa/blob/main/docs/Cluster%20architecture.md).
 */
data class NodeId(val id: String) : Comparable<NodeId> {

    override fun compareTo(other: NodeId): Int = id.compareTo(other.id)
    override fun equals(other: Any?): Boolean = other is NodeId && id == other.id
    override fun hashCode(): Int = id.hashCode()
    override fun toString(): String = "node[$id]"

    companion object {
        /**
         * Placeholder for the deployments that have no cluster and therefore no node to name, internal usage only.
         */
        internal val EMPTY_NODE_ID = NodeId("")
    }
}
