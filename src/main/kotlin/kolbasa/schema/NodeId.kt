package kolbasa.schema

/**
 * Encapsulates the node identifier to avoid confusing other String variables with the node id
 */
internal data class NodeId(val id: String) : Comparable<NodeId> {
    override fun compareTo(other: NodeId): Int = id.compareTo(other.id)
}
