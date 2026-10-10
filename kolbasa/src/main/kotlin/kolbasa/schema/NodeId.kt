package kolbasa.schema

/**
 * Encapsulates the node identifier to avoid confusing other String variables with the node id
 */
internal data class NodeId(val id: String) : Comparable<NodeId> {

    override fun equals(other: Any?): Boolean {
        return other is NodeId && id == other.id
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun compareTo(other: NodeId): Int = id.compareTo(other.id)

    companion object {
        val EMPTY_NODE_ID = NodeId("")
    }
}
