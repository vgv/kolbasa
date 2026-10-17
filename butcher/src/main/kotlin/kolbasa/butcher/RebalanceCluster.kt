package kolbasa.butcher

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.Widget

internal class RebalanceCluster : HighLevelCommand(name = "rebalance-cluster") {

    override fun help(context: Context) =
        "Even out the shard distribution by running the 'shard-balance' plan as repeated migration rounds"

    override fun execute(): Widget {
        val butcher = butcher()
        refuseIfMigrating(butcher)

        val plan = butcher.checkShardBalance()
        if (plan.isBalanced) {
            // Nothing to agree to, and the report says so itself
            return plan.render()
        }

        // The plan goes on screen before the question, so what is being agreed to is visible. It is the
        // same report 'check-cluster --checks=shard-balance' shows
        terminal.println(plan.render())
        confirm(
            "${plan.totalMoves} shard(s) will move, in rounds of $shardsPerRound: each round prepares its " +
                "shards, copies their rows and hands them over before the next one starts, and the shards " +
                "of a round are consumed by no node while their rows are copied."
        )

        butcher.rebalance(shardsPerRound, RebalanceProgressCallback(terminal))

        // Where the cluster ended up, read back rather than predicted
        return butcher.checkShardBalance().render()
    }
}
