package kolbasa.cluster.admin

import kolbasa.cluster.Shard
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * Which node a shard belongs to, and whether it is in the middle of being handed over
 *
 * A shard is always in exactly one of these two states, so a report never has to be read as a combination of
 * nullable fields. See [Cluster architecture](https://github.com/vgv/kolbasa/blob/main/docs/Cluster%20architecture.md).
 */
sealed interface ShardOwnership {

    /** The shard this describes */
    val shard: ShardId

    /** Messages of [shard] are produced to and consumed from [node], and nothing is in flight */
    data class Stable(override val shard: ShardId, val node: NodeId) : ShardOwnership

    /**
     * [shard] is being handed over to [target]: new messages already go there, and nothing consumes the
     * shard until the migration is finalized
     *
     * There is no "from" here on purpose – the node the shard is coming from is not recorded anywhere, so a
     * migration in progress only knows where it is going.
     */
    data class Migrating(override val shard: ShardId, val target: NodeId) : ShardOwnership
}

/**
 * The invariant of the internal record is that a stable shard has a consumer and a migrating one does not,
 * and that the producer of a migrating shard is its future consumer – see `Shard.init`
 */
internal fun Shard.toOwnership(): ShardOwnership {
    return if (consumerNode != null) {
        ShardOwnership.Stable(shard = shard, node = consumerNode)
    } else {
        ShardOwnership.Migrating(shard = shard, target = producerNode)
    }
}
