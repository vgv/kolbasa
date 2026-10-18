package kolbasa.cluster.admin

import kolbasa.cluster.admin.check.MigrationState
import kolbasa.cluster.admin.check.ShardBalance
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Run prepare, move and finalize over and over until there is nothing left to move.
 *
 * One round moves at most [shardsPerRound] shards. The plan comes from [ShardBalance] and is read again
 * before every round, so the loop always works from the current state of the cluster.
 *
 * With [drainedNodes] empty this evens out the whole cluster. With a node named there it empties that node
 * and leaves the rest alone. Both stop when the plan is empty.
 *
 * Tables are never filtered here. A filtered [move] leaves the skipped rows on the old node, and the
 * [finalize] of the same round would then hand the shard over without them.
 */
internal fun rebalance(
    dataSources: List<DataSource>,
    drainedNodes: Set<NodeId>,
    shardsPerRound: Int,
    progressCallback: ProgressCallback
) {
    require(shardsPerRound > 0) {
        "Shards per round must be positive, but was $shardsPerRound"
    }

    // One clear error instead of a connection failure in the middle of a round
    MoveHelpers.checkClusterNodes(dataSources)

    // move() and finalize() act on every migrating shard, not only on the ones prepared here. A migration
    // somebody else left in flight cannot be avoided, so it is refused instead
    val migrationState = MigrationState.check(dataSources)
    if (!migrationState.isClean) {
        throw ClusterOperationException.MigrationAlreadyInFlightException(migrationState.migratingShardsByTarget)
    }

    var round = 0
    var shardsMoved = 0
    var shardsLeftToMove = Int.MAX_VALUE

    while (true) {
        val plan = ShardBalance.check(dataSources, drainedNodes)
        if (plan.isBalanced) {
            break
        }

        // Every round finishes its shards, so the next plan is always shorter. If it is not, something is
        // wrong, and an endless loop is the worst way to report it
        if (plan.totalMoves >= shardsLeftToMove) {
            throw ClusterOperationException.ExecutionException(
                "Rebalance is not converging: ${plan.totalMoves} shard(s) still have to move after " +
                    "round $round, which is no better than before it. Nothing has been left mid-migration, " +
                    "so the cluster is safe to inspect"
            )
        }
        shardsLeftToMove = plan.totalMoves

        val roundMoves = plan.proposedMoves.takeShards(shardsPerRound)
        progressCallback.rebalanceRoundStart(++round, roundMoves, shardsLeftToMove)

        roundMoves.forEach { (target, shards) ->
            prepare(dataSources, target, shards, progressCallback)
        }
        move(dataSources, includeTables = null, excludeTables = null, progressCallback = progressCallback)
        finalize(dataSources, progressCallback)

        shardsMoved += roundMoves.values.sumOf { it.size }
    }

    progressCallback.rebalanceFinished(round, shardsMoved)
}

/**
 * Up to [limit] shards of this plan, target by target
 *
 * One target is filled before the next starts, so a round makes as few [prepare] calls as possible.
 */
private fun Map<NodeId, List<ShardId>>.takeShards(limit: Int): Map<NodeId, List<ShardId>> {
    var left = limit

    return buildMap {
        for ((target, shards) in this@takeShards) {
            if (left == 0) {
                break
            }
            val slice = shards.sorted().take(left)
            put(target, slice)
            left -= slice.size
        }
    }
}
