package kolbasa.cluster.schema

import kolbasa.cluster.Shard

internal data class Node(
    val serverId: String,
    val identifierBucket: Int?
) : Comparable<Node> {

    override fun compareTo(other: Node): Int {
        return this.serverId.compareTo(other.serverId)
    }

    companion object {
        const val BUCKET_FOR_NOT_CLUSTERED_ENVIRONMENT = 0

        const val MIN_BUCKET = 1
        const val MAX_BUCKET = Shard.SHARD_COUNT

        val BITS_TO_HOLD_BUCKET_VALUE = Int.SIZE_BITS - MAX_BUCKET.countLeadingZeroBits()
        val BITS_TO_HOLD_ID_VALUE = (Long.SIZE_BITS - 1) - BITS_TO_HOLD_BUCKET_VALUE

        val VALUE_MASK = (1.toLong() shl BITS_TO_HOLD_ID_VALUE) - 1
    }
}
