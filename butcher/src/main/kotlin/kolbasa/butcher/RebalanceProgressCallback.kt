package kolbasa.butcher

import com.github.ajalt.mordant.terminal.Terminal
import kolbasa.cluster.admin.ProgressCallback
import kolbasa.cluster.admin.ShardDiff
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * Progress of a rebalance or a node drain
 *
 * A round moves hundreds of shards, and the rebalance chose them, not the operator. A line per shard would
 * bury the rounds, so prepare and finalize print a count. Copying rows prints the same as a step run by hand.
 */
internal class RebalanceProgressCallback(
    private val terminal: Terminal
) : ProgressCallback by ConsoleProgressCallback(terminal) {

    override fun rebalanceRoundStart(round: Int, moves: Map<NodeId, List<ShardId>>, shardsLeftToMove: Int) {
        val targets = moves.entries
            .sortedBy { it.key }
            .joinToString(separator = ", ") { (target, shards) -> "${count(shards.size.toLong())} to ${target.id}" }

        terminal.println("")
        terminal.println("Round $round: $targets (${count(shardsLeftToMove.toLong())} shard(s) left to move)")
    }

    override fun prepareSuccessful(shards: List<ShardId>, targetNode: NodeId, shardsDiff: List<ShardDiff>) {
        terminal.println("Prepared ${count(shardsDiff.size.toLong())} shard(s) for ${targetNode.id}")
    }

    override fun finalizeSuccessful(shardsDiff: List<ShardDiff>) {
        terminal.println("Finalized ${count(shardsDiff.size.toLong())} shard(s)")
    }

    override fun rebalanceFinished(rounds: Int, shardsMoved: Int) {
        terminal.println("")
        terminal.println("Moved ${count(shardsMoved.toLong())} shard(s) in ${count(rounds.toLong())} round(s)")
    }
}
