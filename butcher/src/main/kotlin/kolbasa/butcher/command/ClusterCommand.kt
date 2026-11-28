package kolbasa.butcher.command

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.CliktError
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import kolbasa.butcher.HelpSection
import kolbasa.butcher.PROGRAM_NAME
import kolbasa.butcher.config.ClusterConfig
import kolbasa.butcher.config.PgEndpoint
import kolbasa.cluster.admin.ClusterAdmin
import kolbasa.cluster.admin.ClusterAdminHelpers
import kolbasa.cluster.admin.ClusterOperationException

/**
 * A command that acts on a cluster described by one or more config files
 *
 * Everything every command shares: the cluster it works on, and how the cluster is checked before the work starts.
 * The config files are given to `butcher` itself with `-c`, before the command, see [kolbasa.butcher.ButcherCli].
 */
internal abstract class ClusterCommand(name: String) : CliktCommand(name = name) {

    /**
     * The section of `butcher --help` this command is listed under
     */
    protected abstract val helpSection: HelpSection

    final override val helpTags: Map<String, String>
        get() = HelpSection.helpTags(helpSection)

    private val cluster by requireObject<ClusterConfig>()

    /**
     * Whether a successful run starts its output with "Completed successfully."
     *
     * [CheckCluster] ends with a line of its own instead: a check can run and still find something.
     */
    protected open val reportsCompletion: Boolean = true

    override fun helpEpilog(context: Context) =
        "The cluster is given with -c, before the command: $PROGRAM_NAME -c cluster.conf $commandName. " +
            "See '$PROGRAM_NAME --help' for -c and the config file format."

    final override fun run() {
        val output = try {
            execute()
        } catch (e: ClusterOperationException) {
            // Everything the library refuses to do, shown as a message instead of a stack trace. Every
            // subclass carries a message, so nothing is lost by not telling them apart here
            throw CliktError(e.message)
        }

        if (reportsCompletion) {
            echo(TextColors.green("Completed successfully."))
        }
        output?.let { terminal.println(it) }
    }

    /**
     * Does the work, and returns what to show after it, if anything
     */
    protected abstract fun execute(): Widget?

    /**
     * Builds the operator handle for the cluster the config files describe
     *
     * Opening the data sources happens here and not in the constructor, so that `--help` never touches a
     * database.
     *
     * Every node must also have the id on its config line. [ClusterAdmin] checks that on every call. Only
     * [InitNodes] works on a node that has no id yet.
     */
    protected fun butcher(): ClusterAdmin {
        val endpoints = cluster.endpoints

        // Every node is asked whether it is there before anything else happens, and asked on a plain
        // driver connection: a pool would wait out its timeout and then blame itself
        ClusterAdminHelpers.checkNodesAvailable(endpoints.map(PgEndpoint::driverDataSource))

        return ClusterAdmin(endpoints.associate { endpoint -> endpoint.id to endpoint.pooledDataSource() })
    }
}

