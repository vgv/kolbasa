package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.split

internal class MoveData : ClusterCommand(name = "move-data") {

    private val includeTables by option("--include-tables", metavar = "<t1,t2,...>")
        .split(",")
        .help("Queue tables to migrate, comma-separated. All of them by default")

    private val excludeTables by option("--exclude-tables", metavar = "<t1,t2,...>")
        .split(",")
        .help("Queue tables not to migrate, comma-separated. None by default")

    override fun help(context: Context) =
        "Transfer data to target nodes for every shard in migration state. Safe to re-run if it crashed"

    override fun execute(): String? {
        butcher().moveData(
            includeTables = includeTables?.map { it.trim() }?.toSet(),
            excludeTables = excludeTables?.map { it.trim() }?.toSet(),
            progress = ConsoleProgressCallback
        )
        return null
    }
}
