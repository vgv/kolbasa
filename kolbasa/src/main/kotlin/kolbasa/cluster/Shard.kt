package kolbasa.cluster

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import java.time.Instant
import kotlin.random.Random

internal class Shard(
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

    override fun equals(other: Any?): Boolean {
        return other is Shard &&
            shard == other.shard &&
            producerNode == other.producerNode &&
            consumerNode == other.consumerNode &&
            nextConsumerNode == other.nextConsumerNode &&
            revision == other.revision
    }

    override fun hashCode(): Int {
        var result = shard.hashCode()
        result = 31 * result + producerNode.hashCode()
        result = 31 * result + consumerNode.hashCode()
        result = 31 * result + nextConsumerNode.hashCode()
        result = 31 * result + revision.hashCode()
        return result
    }

    override fun toString(): String =
        "shard[${shard.id},${producerNode.id},${consumerNode?.id},${nextConsumerNode?.id},$revision]"

    /**
     * Whether [other] has the same fields as this row except revision
     */
    fun equalsIgnoreRevision(other: Shard): Boolean {
        return shard == other.shard &&
            producerNode == other.producerNode &&
            consumerNode == other.consumerNode &&
            nextConsumerNode == other.nextConsumerNode
    }

    companion object {
        fun randomShard(): Int {
            return Random.nextInt(ShardId.MIN_SHARD, ShardId.MAX_SHARD + 1)
        }
    }
}

