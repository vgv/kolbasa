package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.butcher.HelpSection
import kolbasa.butcher.render
import kolbasa.cluster.admin.InitClusterPlan

internal class InitCluster : MutatingClusterCommand(name = "init-cluster") {

    override val helpSection = HelpSection.SETUP

    override fun help(context: Context) =
        "Set up a new cluster, or a new node of one: give the nodes their ids, give every shard a node and bring the " +
            "copies of the shard map up to date. The same as init-nodes, init-shards and sync-shards, with one question"

    override fun execute(): Widget {
        val butcher = butcher()

        val plan = butcher.planInitCluster()
        if (plan.isEmpty) {
            // Nothing to agree to, and the report says how the copies are
            return verticalLayout {
                cell(plan.render())
                cell(butcher.checkShardMap().render())
            }
        }

        // The plan goes on screen before the question, so what is being agreed to is visible
        terminal.println(plan.render())
        confirm(describe(plan))

        val result = butcher.initCluster()

        // Where the copies ended up, read back rather than predicted
        val shardMapAfter = butcher.checkShardMap()
        return verticalLayout {
            result.initializedNodes.forEach { node ->
                cell("Initialized node ${node.id}")
            }
            val shardsPerNode = result.owners.values.groupingBy { node -> node }.eachCount().toSortedMap()
            shardsPerNode.forEach { (node, shards) ->
                cell("${node.id}: got $shards shard(s)")
            }
            result.changedRows.forEach { (node, rows) ->
                if (rows > 0) {
                    cell("${node.id}: $rows row(s) of the shard map changed")
                }
            }
            cell(shardMapAfter.render())
        }
    }

    private fun describe(plan: InitClusterPlan): String {
        val parts = mutableListOf<String>()
        if (!plan.nodes.isEmpty) {
            parts += "${plan.nodes.newNodes.size} node(s) will get an id"
        }
        if (!plan.shards.isEmpty) {
            parts += "${plan.shards.owners.size} shard(s) will get a node"
        }
        if (plan.copiesToSync > 0) {
            val copies = plan.nodes.newNodes.size + plan.nodes.initializedNodes.size
            parts += "${plan.copiesToSync} of $copies copies of the shard map will be brought up to date"
        }
        return parts.joinToString(", ") + "."
    }
}
