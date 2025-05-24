package kolbasa.schema

import kolbasa.cluster.Shard
import kotlin.random.Random

internal data class Node(
    val id: String,
    val identifiersBucket: Int
) : Comparable<Node> {

    override fun compareTo(other: Node): Int {
        return this.id.compareTo(other.id)
    }

    companion object {
        const val MIN_BUCKET = Shard.MIN_SHARD
        const val MAX_BUCKET = Shard.MAX_SHARD

        const val BITS_TO_HOLD_BUCKET_VALUE = Shard.SHARD_BITS  // 10 bits
        const val BITS_TO_HOLD_ID_VALUE = Long.SIZE_BITS - BITS_TO_HOLD_BUCKET_VALUE - 1 // 53 bits

        const val VALUE_MASK = (1.toLong() shl BITS_TO_HOLD_ID_VALUE) - 1 // 53 bits filled with 1

        fun randomBucket(): Int {
            return Random.nextInt(MIN_BUCKET, MAX_BUCKET + 1)
        }
    }
}
