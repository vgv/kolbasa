package kolbasa.cluster

import kotlin.random.Random

data class Shard(
    val shard: Int,
    val producerNode: String,
    val consumerNode: String?,
    val nextConsumerNode: String?
) {

    init {
        check((producerNode == consumerNode && nextConsumerNode == null) || (producerNode == nextConsumerNode && consumerNode == null)) {
            "Invalid shard state: producerNode=$producerNode, consumerNode=$consumerNode, nextConsumerNode=$nextConsumerNode"
        }

        check(shard in MIN_SHARD..MAX_SHARD) {
            "Invalid shard value: $shard, possible values: [$MIN_SHARD..$MAX_SHARD]"
        }
    }

    companion object {
        const val MIN_SHARD = 0
        const val MAX_SHARD = 1023
        const val SHARD_COUNT = MAX_SHARD - MIN_SHARD + 1

        fun randomShard(): Int {
            return Random.nextInt(MIN_SHARD, MAX_SHARD + 1)
        }
    }

}
