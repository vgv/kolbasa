package kolbasa.cluster

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import java.time.Instant
import kotlin.random.Random

internal data class Shard(
    val shard: ShardId,
    val producerNode: NodeId,
    val consumerNode: NodeId?,
    val nextConsumerNode: NodeId?,
    // A simple kind of hybrid logical clock: close to the time of the last change, but not exactly it.
    val revision: Instant
) {

    init {
        val stableState = (producerNode == consumerNode) && (nextConsumerNode == null)
        val migrationState = (producerNode == nextConsumerNode) && (consumerNode == null)

        check(stableState xor migrationState) {
            "Invalid shard state: producerNode=$producerNode, consumerNode=$consumerNode, nextConsumerNode=$nextConsumerNode"
        }
    }

    /**
     * Whether [other] has the same fields as this row except revision
     */
    fun equalsIgnoreRevision(other: Shard): Boolean {
        return this.shard == other.shard &&
            this.producerNode == other.producerNode &&
            this.consumerNode == other.consumerNode &&
            this.nextConsumerNode == other.nextConsumerNode
    }

    companion object {
        fun randomShard(): Int {
            return Random.nextInt(ShardId.MIN_SHARD, ShardId.MAX_SHARD + 1)
        }
    }
}

