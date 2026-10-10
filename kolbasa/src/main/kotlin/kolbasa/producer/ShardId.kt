package kolbasa.producer

import kotlin.math.abs
import kotlin.random.Random

class ShardId private constructor(
    val id: Int
) : Comparable<ShardId> {

    init {
        check(id in MIN_SHARD..MAX_SHARD) {
            "Invalid shard value: $id, possible values: [$MIN_SHARD..$MAX_SHARD]"
        }
    }

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
        val MIN_SHARD_ID = CACHE[MIN_SHARD]

        const val MAX_SHARD = (1 shl SHARD_BITS) - 1
        val MAX_SHARD_ID = CACHE[MAX_SHARD]

        val SHARDS_RANGE = MIN_SHARD..MAX_SHARD
        val SHARDS_ID_RANGE = MIN_SHARD_ID .. MAX_SHARD_ID

        @JvmStatic
        fun of(shard: Int): ShardId {
            val effectiveShard = abs(shard % SHARD_COUNT)

            return CACHE[effectiveShard]
        }

        @JvmStatic
        fun random(): ShardId = CACHE[Random.nextInt(SHARD_COUNT)]
    }
}
