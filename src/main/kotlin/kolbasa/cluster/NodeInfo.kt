package kolbasa.cluster

internal data class NodeInfo(
    val serverId: String,
    val sendEnabled: Boolean,
    val receiveEnabled: Boolean
) : Comparable<NodeInfo> {

    private val compareKey = "$serverId-$sendEnabled-$receiveEnabled"

    override fun compareTo(other: NodeInfo): Int {
        return compareKey compareTo other.compareKey
    }

}
