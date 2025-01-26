package kolbasa.cluster.schema

data class Node(
    val serverId: String,
    val identifierBucket: Int?
) : Comparable<Node> {

    override fun compareTo(other: Node): Int {
        return this.serverId.compareTo(other.serverId)
    }
}
