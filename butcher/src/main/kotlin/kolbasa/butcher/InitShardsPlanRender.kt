package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.InitShardsPlan

/**
 * Renders an [InitShardsPlan] the way butcher shows it before the question
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun InitShardsPlan.render(): Widget = verticalLayout {
    if (isEmpty) {
        cell(TextColors.green("Every shard has a node already"))
        return@verticalLayout
    }

    val shardsPerNode = owners.values.groupingBy { node -> node }.eachCount().toSortedMap()
    shardsPerNode.forEach { (node, shards) ->
        cell("${node.id}: gets $shards shard(s)")
    }

    unsafeNodes.forEach { node ->
        cell(
            TextColors.yellow(
                "${node.id} has messages, and the cluster has no shard map yet. Creating it together with other nodes " +
                    "is unsafe: the messages of the shards that go to other nodes stay unread. To grow a standalone " +
                    "node into a cluster, give it every shard with --target=${node.id}, then rebalance the cluster."
            )
        )
    }
}
