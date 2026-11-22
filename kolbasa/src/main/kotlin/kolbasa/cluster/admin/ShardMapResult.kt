package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * How the copy of the shard map on one node differs from the newest rows of all the copies
 *
 * @property missing the copy has no row for these shards
 * @property outdated the copy has an older row for these shards
 * @property conflicting the copy has another row for these shards, with the same revision as the newest one
 */
data class ShardCopyResult(
    val missing: List<ShardId>,
    val outdated: List<ShardId>,
    val conflicting: List<ShardId>,
) {

    /**
     * True when the copy has the newest row of every shard
     */
    val isUpToDate: Boolean = missing.isEmpty() && outdated.isEmpty() && conflicting.isEmpty()
}

/**
 * The state of the shard map: every node keeps a copy of it, and for every shard the newest row of all the copies is the
 * right one
 *
 * A copy that lags behind is not an error by itself. Applications bring the copies up to date on every topology update, so
 * right after a change, by hand or by butcher, some of them lag for a while. A copy that stays behind is on a node that
 * takes no writes, or no application works with the cluster.
 *
 * @property copies how every copy differs from the newest rows, for the nodes that have a shard table
 * @property nodesWithoutTable the nodes that have no shard table
 * @property shardsWithoutRow the shards that have no row on any node
 * @property invalidShards rows with a shard number that doesn't exist, per node. Nobody reads such rows
 * @property unknownOwners nodes that own shards but are not among the nodes of the cluster, with their shards
 */
data class ShardMapResult(
    val copies: Map<NodeId, ShardCopyResult>,
    val nodesWithoutTable: Set<NodeId>,
    val shardsWithoutRow: Set<ShardId>,
    val invalidShards: Map<NodeId, List<Int>>,
    val unknownOwners: Map<NodeId, List<ShardId>>,
) {

    /**
     * True when no shard has a row on any node: the cluster has no shard map yet
     */
    val isMissing: Boolean = shardsWithoutRow.size == ShardId.SHARD_COUNT

    /**
     * How many nodes don't have the newest row of every shard, the nodes without a shard table among them
     */
    val differentCopies: Int = copies.values.count { !it.isUpToDate } + nodesWithoutTable.size

    /**
     * True when every node has a shard table and every shard has a row
     */
    val isComplete: Boolean = nodesWithoutTable.isEmpty() && shardsWithoutRow.isEmpty()

    /**
     * True when the map is complete, all its copies are the same, and nothing else is wrong with it
     */
    val isClean: Boolean = isComplete &&
        differentCopies == 0 &&
        invalidShards.isEmpty() &&
        unknownOwners.isEmpty()
}
