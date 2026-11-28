package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import kolbasa.butcher.HelpSection
import kolbasa.butcher.render

internal class InitNodes : MutatingClusterCommand(name = "init-nodes") {

    override val helpSection = HelpSection.SETUP

    override fun help(context: Context) =
        "Give every node that has no id yet the id from its line. Run it for a new node before an application's " +
            "Cluster sees it: a Cluster gives such a node a random id"

    override fun execute(): Widget {
        val butcher = butcher()

        val plan = butcher.planInitNodes()
        if (plan.isEmpty) {
            // Nothing to agree to, and the plan says so itself
            return plan.render()
        }

        // The plan goes on screen before the question, so what is being agreed to is visible
        terminal.println(plan.render())
        confirm("${plan.newNodes.size} node(s) will get an id.")

        val initialized = butcher.initNodes()
        return verticalLayout {
            initialized.forEach { node ->
                cell("Initialized node ${node.id}")
            }
        }
    }
}
