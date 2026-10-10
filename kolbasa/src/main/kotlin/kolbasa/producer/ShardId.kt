package kolbasa.producer

import kotlin.random.Random

class ShardId private constructor(
    val id: Int
) : Comparable<ShardId> {

    override fun compareTo(other: ShardId) = id.compareTo(other.id)
    override fun equals(other: Any?) = other is ShardId && id == other.id
    override fun hashCode() = id
    override fun toString() = "shard[$id]"

    companion object {
        internal const val SHARD_BITS = 10
        const val SHARD_COUNT = 1 shl SHARD_BITS

        private val CACHE = Array(SHARD_COUNT) { index ->
            ShardId(index)
        }

        const val MIN_SHARD = 0
        const val MAX_SHARD = (1 shl SHARD_BITS) - 1
        val SHARDS_RANGE = MIN_SHARD..MAX_SHARD

        @JvmStatic
        fun of(shard: Int): ShardId {
            check(shard in SHARDS_RANGE) {
                "Invalid shard value: $shard, possible values: [$MIN_SHARD..$MAX_SHARD]"
            }

            return CACHE[shard]
        }

        internal fun random(): ShardId = CACHE[Random.nextInt(SHARD_COUNT)]
    }
}
