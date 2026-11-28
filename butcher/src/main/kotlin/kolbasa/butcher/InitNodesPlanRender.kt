package kolbasa.butcher

import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.cluster.admin.InitNodesPlan

/**
 * Renders an [InitNodesPlan] the way butcher shows it before the question
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun InitNodesPlan.render(): Widget = verticalLayout {
    if (isEmpty) {
        cell(TextColors.green("Every node has its id already"))
        return@verticalLayout
    }

    newNodes.forEach { node ->
        cell("${node.id.id}: gets its id, with bucket ${node.bucket}")
    }
    initializedNodes.forEach { node ->
        cell("${node.id}: has its id already")
    }

    unsafeNodes.forEach { node ->
        cell(
            TextColors.yellow(
                "${node.id} has messages but no id yet. Initializing it together with other nodes is unsafe: its " +
                    "messages of shards that go to other nodes stay unread. To grow a standalone node into a cluster, " +
                    "init it alone first, see Butcher.md."
            )
        )
    }
}
