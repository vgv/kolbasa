package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.butcher.HelpSection
import kolbasa.butcher.render

internal class InitShards : MutatingClusterCommand(name = "init-shards") {

    override val helpSection = HelpSection.SETUP

    override fun help(context: Context) =
        "Give every shard that has no row on any node the node with the fewest shards. On a new cluster that creates the " +
            "shard map, spread evenly over the nodes. Applications do the same on every topology update, so this is for " +
            "a cluster with no application running"

    override fun execute(): Widget {
        val butcher = butcher()

        val plan = butcher.planInitShards()
        if (plan.isEmpty) {
            // Nothing to agree to. Copies that differ are for sync-shards, and the report says so
            return verticalLayout {
                cell(plan.render())
                cell(butcher.checkShardMap().render())
            }
        }

        // The plan goes on screen before the question, so what is being agreed to is visible
        terminal.println(plan.render())
        confirm("${plan.owners.size} shard(s) will get a node.")

        val owners = butcher.initShards()

        // Where the copies ended up, read back rather than predicted
        val shardMapAfter = butcher.checkShardMap()
        return verticalLayout {
            val shardsPerNode = owners.values.groupingBy { node -> node }.eachCount().toSortedMap()
            shardsPerNode.forEach { (node, shards) ->
                cell("${node.id}: got $shards shard(s)")
            }
            cell(shardMapAfter.render())
        }
    }
}
