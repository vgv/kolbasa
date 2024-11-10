package kolbasa.cluster

internal data class NodeInfo(
    val serverId: String,
    val sendEnabled: Boolean,
    val receiveEnabled: Boolean
) : Comparable<NodeInfo> {

    override fun compareTo(other: NodeInfo): Int {
        return this.serverId.compareTo(other.serverId)
    }

}
