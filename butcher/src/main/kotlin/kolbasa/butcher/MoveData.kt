package kolbasa.butcher

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.split
import com.github.ajalt.mordant.rendering.Widget

internal class MoveData : MutatingClusterCommand(name = "move-data") {

    private val includeTables by option("--include-tables", metavar = "<t1,t2,...>")
        .split(",")
        .help("Queue tables to migrate, comma-separated. All of them by default")

    private val excludeTables by option("--exclude-tables", metavar = "<t1,t2,...>")
        .split(",")
        .help("Queue tables not to migrate, comma-separated. None by default")

    override val helpSection = HelpSection.LOW_LEVEL

    override fun help(context: Context) =
        "Transfer data to target nodes for every shard in migration state. Safe to re-run if it crashed"

    override fun execute(): Widget? {
        confirm("The rows of every shard that is currently migrating will be copied to the node taking it over. " +
            "This can take a long time, and is safe to re-run if interrupted.")

        butcher().moveData(
            includeTables = includeTables?.map { it.trim() }?.toSet(),
            excludeTables = excludeTables?.map { it.trim() }?.toSet(),
            progress = ConsoleProgressCallback(terminal)
        )
        return null
    }
}
