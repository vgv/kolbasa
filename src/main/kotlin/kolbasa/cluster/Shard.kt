package kolbasa.cluster

import kolbasa.schema.NodeId
import kotlin.random.Random

internal data class Shard(
    val shard: Int,
    val producerNode: NodeId,
    val consumerNode: NodeId?,
    val nextConsumerNode: NodeId?
) {

    init {
        val stableState = (producerNode == consumerNode) && (nextConsumerNode == null)
        val migrationState = (producerNode == nextConsumerNode) && (consumerNode == null)
        check(stableState xor migrationState) {
            "Invalid shard state: producerNode=$producerNode, consumerNode=$consumerNode, nextConsumerNode=$nextConsumerNode"
        }

        check(shard in MIN_SHARD..MAX_SHARD) {
            "Invalid shard value: $shard, possible values: [$MIN_SHARD..$MAX_SHARD]"
        }
    }

    companion object {
        // 10 bits means that there can be 1024 shards, that is, we can distribute the load across a cluster of 1024 servers
        const val SHARD_BITS = 10
        const val MIN_SHARD = 0
        const val MAX_SHARD = (1 shl SHARD_BITS) - 1  // 1023
        const val SHARD_COUNT = 1 shl SHARD_BITS      // 1024

        fun randomShard(): Int {
            return Random.nextInt(MIN_SHARD, MAX_SHARD + 1)
        }
    }
}

