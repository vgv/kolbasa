package kolbasa.schema

import kolbasa.producer.ShardId
import kotlin.random.Random

internal class Node(
    val id: NodeId,
    val identifiersBucket: Int
) : Comparable<Node> {

    override fun compareTo(other: Node): Int = id.compareTo(other.id)
    override fun equals(other: Any?): Boolean = other is Node && id == other.id
    override fun hashCode(): Int = id.hashCode()
    override fun toString(): String = "node[${id.id},$identifiersBucket]"

    companion object {
        const val MIN_BUCKET = ShardId.MIN_SHARD
        const val MAX_BUCKET = ShardId.MAX_SHARD

        const val BITS_TO_HOLD_BUCKET_VALUE = ShardId.SHARD_BITS  // 10 bits
        const val BITS_TO_HOLD_ID_VALUE = Long.SIZE_BITS - BITS_TO_HOLD_BUCKET_VALUE - 1 // 53 bits

        const val VALUE_MASK = (1.toLong() shl BITS_TO_HOLD_ID_VALUE) - 1 // 53 bits filled with 1

        fun randomBucket(): Int {
            return Random.nextInt(MIN_BUCKET, MAX_BUCKET + 1)
        }
    }
}
