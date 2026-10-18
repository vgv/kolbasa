package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.split
import com.github.ajalt.mordant.rendering.Widget
import kolbasa.butcher.RebalanceProgressCallback
import kolbasa.butcher.renderNodesDrain
import kolbasa.schema.NodeId

internal class DrainNodes : HighLevelCommand(name = "drain-nodes") {

    private val nodes: List<NodeId> by option("--nodes", metavar = "<node-id,node-id,...>")
        .convert { NodeId(it) }
        .split(",")
        .required()
        .help("Comma-separated nodes to empty, named by the node ids the cluster knows them as")

    override fun help(context: Context) =
        "Move everything off one or more nodes so they can leave the cluster, in repeated migration rounds"

    override fun execute(): Widget {
        val drainedNodes = nodes.toSet()
        val butcher = butcher()
        refuseIfMigrating(butcher)

        val plan = butcher.planNodesDrain(drainedNodes)
        if (plan.isBalanced) {
            // Already empty, which is all the command is after
            return plan.renderNodesDrain(drainedNodes)
        }

        // The plan goes on screen before the question, so what is being agreed to is visible
        terminal.println(plan.renderNodesDrain(drainedNodes))
        confirm(
            "${plan.totalMoves} shard(s) will move off ${drainedNodes.sorted().joinToString(", ") { it.id }}, " +
                "in rounds of $shardsPerRound. They go to the other nodes, but no shards move between those " +
                "nodes. If the other nodes are uneven now, they may stay uneven. 'rebalance-cluster' evens " +
                "them out."
        )

        butcher.drainNodes(drainedNodes, shardsPerRound, RebalanceProgressCallback(terminal))

        // Where the nodes ended up, read back rather than predicted
        return butcher.planNodesDrain(drainedNodes).renderNodesDrain(drainedNodes)
    }
}
