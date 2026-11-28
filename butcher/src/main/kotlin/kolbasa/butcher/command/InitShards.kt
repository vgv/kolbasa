package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.check
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.split
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.butcher.HelpSection
import kolbasa.butcher.render
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

internal class InitShards : MutatingClusterCommand(name = "init-shards") {

    private val target by option("--target", metavar = "<node-id>")
        .convert { NodeId(it) }
        .help("Give the shards to this node, an id from the config. Without it they are spread evenly")

    private val shards by option("--shards", metavar = "<0,1,2,...>")
        .int()
        .split(",")
        .help("Comma-separated shard numbers: only these shards, and none of them may have a row. Needs --target")
        .check(
            lazyMessage = { out ->
                "shard(s) out of range [${ShardId.MIN_SHARD}..${ShardId.MAX_SHARD}]: " +
                    out.filterNot { it in ShardId.SHARDS_RANGE }.joinToString(", ")
            },
            validator = { shards ->
                shards.all { it in ShardId.SHARDS_RANGE }
            })

    override val helpSection = HelpSection.SETUP

    override fun help(context: Context) =
        "Give every shard that has no row on any node a node: the one with the fewest shards, or the --target node. On a " +
            "new cluster that creates the shard map. Applications give such shards a node on every topology update, so " +
            "this is for a cluster with no application running"

    override fun execute(): Widget {
        val target = target
        val shards = shards?.map(ShardId::of)
        // Before anything else, the way Clikt checks the options themselves. With this command's context, the usage line
        // is this command's, not butcher's
        if (shards != null && target == null) {
            throw UsageError("--shards needs --target: the node to give these shards to").apply { context = currentContext }
        }

        val butcher = butcher()

        val plan = if (target == null) butcher.planInitShards() else butcher.planInitShards(target, shards)
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

        val owners = if (target == null) butcher.initShards() else butcher.initShards(target, shards)

        // Where the copies ended up, read back rather than predicted
        val shardMapAfter = butcher.checkShardMap()
        return verticalLayout {
            val shardsPerNode = owners.values.groupingBy { node -> node }.eachCount().toSortedMap()
            shardsPerNode.forEach { (node, shards) ->
                cell("${node.id}: got $shards shard(s)")
            }
            if (target != null) {
                lostShards(owners, target).forEach { (node, shards) ->
                    cell(
                        TextColors.yellow(
                            "Shard(s) ${shards.joinToString(",") { it.id.toString() }} went to ${node.id}: an " +
                                "application gave them a node at the same moment"
                        )
                    )
                }
            }
            cell(shardMapAfter.render())
        }
    }

    // The shards that another node got instead of the target, by that node
    private fun lostShards(owners: Map<ShardId, NodeId>, target: NodeId): Map<NodeId, List<ShardId>> {
        val result = sortedMapOf<NodeId, MutableList<ShardId>>()
        for ((shard, node) in owners) {
            if (node != target) {
                result.getOrPut(node) { mutableListOf() } += shard
            }
        }
        return result
    }
}
