package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.CliktError
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.help
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.types.file
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import kolbasa.butcher.Butcher
import kolbasa.butcher.ButcherException
import kolbasa.butcher.cli.config.ClusterNodes
import kolbasa.butcher.cli.config.InvalidConfigurationException

/**
 * A command that acts on a cluster described by one or more config files
 *
 * Everything the four commands share: where the cluster comes from, and the help text describing the file
 * format they all read.
 */
internal abstract class ClusterCommand(name: String) : CliktCommand(name = name) {

    private val configFiles by argument(name = "config-file")
        .file(mustExist = true, canBeDir = false, mustBeReadable = true)
        .multiple(required = true)
        .help("One or more cluster config files, merged by node id")

    override fun helpEpilog(context: Context) = CONFIG_FILE_FORMAT_HELP

    final override fun run() {
        val output = try {
            execute()
        } catch (e: ButcherException) {
            // Everything the library refuses to do, shown as a message instead of a stack trace. Every
            // subclass carries a message, so nothing is lost by not telling them apart here
            throw CliktError(e.message)
        }

        echo(TextColors.green("Completed successfully."))
        output?.let { terminal.println(it) }
    }

    /** Does the work, and returns what to show after it, if anything */
    protected abstract fun execute(): Widget?

    /**
     * Builds the operator handle for the cluster the config files describe
     *
     * Reading the files and opening the data sources happens here and not in the constructor, so that
     * `--help` never touches a file.
     */
    protected fun butcher(): Butcher {
        val nodes = try {
            ClusterNodes.buildClusterNodes(configFiles)
        } catch (e: InvalidConfigurationException) {
            // A bad config file is bad input, so it gets the usage treatment rather than a bare error
            throw UsageError(e.message)
        }

        return Butcher(nodes.dataSources)
    }
}


private val CONFIG_FILE_FORMAT_HELP = """
    A config file holds one node per line: the node id, then libpq-style keys -
    `node-01  host=db01.internal port=5432 user=app dbname=orders schema=public`. Supported keys are host,
    port, user, password, dbname and schema; unknown keys are ignored, values may be single-quoted, and
    lines starting with '#' are skipped.

    Several files are merged by node id, which is how a topology file and a secrets file can be kept apart.

    See https://github.com/vgv/kolbasa/blob/main/docs/Butcher.md for the full description.
""".trimIndent()
