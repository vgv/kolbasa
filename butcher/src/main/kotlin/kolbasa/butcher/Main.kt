package kolbasa.butcher

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.core.obj
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.versionOption
import com.github.ajalt.clikt.parameters.types.file
import kolbasa.butcher.command.CheckCluster
import kolbasa.butcher.command.DrainNodes
import kolbasa.butcher.command.FinalizeMigration
import kolbasa.butcher.command.InitCluster
import kolbasa.butcher.command.InitNodes
import kolbasa.butcher.command.InitShards
import kolbasa.butcher.command.MoveData
import kolbasa.butcher.command.PrepareMigration
import kolbasa.butcher.command.RebalanceCluster
import kolbasa.butcher.command.SyncShards
import kolbasa.butcher.config.ClusterConfig
import kolbasa.butcher.config.ConfigHelper
import kolbasa.butcher.config.InvalidConfigurationException
import java.lang.invoke.MethodHandles

fun main(args: Array<String>) = ButcherCli()
    .subcommands(
        CheckCluster(),
        InitCluster(),
        InitNodes(),
        InitShards(),
        SyncShards(),
        RebalanceCluster(),
        DrainNodes(),
        PrepareMigration(),
        MoveData(),
        FinalizeMigration(),
    )
    .main(args)

/**
 * How butcher is named in its help and its messages
 */
internal const val PROGRAM_NAME = "java -jar butcher.jar"

/**
 * The `butcher` command itself: it reads the cluster config and holds the real commands
 */
internal class ButcherCli : CliktCommand(name = PROGRAM_NAME) {

    private val configFiles by option("-c", "--config", metavar = "<file>")
        .file(mustExist = true, canBeDir = false, mustBeReadable = true)
        .multiple(required = true)
        .help("Cluster config file. Repeat it to merge several files by node id")

    // A bare 'butcher' is a question: it prints the help and exits 0, instead of "missing option --config"
    override val printHelpOnEmptyArgs = true

    init {
        versionOption(butcherVersion(), message = { it })
        context {
            // The only thing it changes is that the commands are listed in sections, see HelpSection
            helpFormatter = { ButcherHelpFormatter(it) }
        }
    }

    override fun help(context: Context) =
        "Inspect and manage a kolbasa cluster."

    override fun helpEpilog(context: Context) = ROOT_HELP_EPILOG

    override fun run() {
        val endpoints = try {
            ConfigHelper.endpoints(configFiles)
        } catch (e: InvalidConfigurationException) {
            // A bad config file is bad input, so it gets the usage treatment rather than a bare error
            throw UsageError(e.message)
        }

        // Read once, here. Every command works on this cluster, see ClusterCommand
        currentContext.obj = ClusterConfig(endpoints)
    }
}

private val ROOT_HELP_EPILOG = """
    The cluster comes from -c, given before the command: `$PROGRAM_NAME -c cluster.conf check-cluster`.
    Several -c are merged by node id, which is how a topology file and a secrets file can be kept apart.

    A config file holds one node per line: the node id, then libpq-style keys -
    `node-01  host=db01.internal port=5432 user=app dbname=orders schema=public`. Supported keys are host,
    port, user, password, dbname and schema; unknown keys are ignored, values may be single-quoted, and
    lines starting with '#' are skipped.

    The node id is the node's own id, kept in its q__node table. 'init-cluster' and 'init-nodes' give a
    node that has none yet the one from its line, and every other command refuses such a node.

    A command of 'Moving shards' is built from the step-by-step ones and nothing else: it works out the
    same shard-balance plan that 'check-cluster' reports, then prepares, moves and finalizes a batch of
    shards at a time until nothing is left to move. The step-by-step commands stay yours to drive
    directly - for choosing the shards yourself, for moving only some tables, or for finishing a migration
    that 'rebalance-cluster' or 'drain-nodes' refuses to touch.

    See https://github.com/vgv/kolbasa/blob/main/docs/Butcher.md for the full description.
""".trimIndent()

/**
 * The version of the jar this code was loaded from
 *
 * `MethodHandles.lookup().lookupClass()` is the idiomatic JVM way to get a Class handle to "the class
 * containing this code" without declaring a placeholder type. It resolves to the synthetic `MainKt` class
 * that Kotlin generates for the top-level functions of this file, whose `Package` carries the jar
 * manifest's `Implementation-Version` attribute set by the butcherJar task. Falls back to "unknown" outside
 * a jar (an IDE, `./gradlew run`), where the JVM does not populate the attribute.
 */
private fun butcherVersion(): String {
    return MethodHandles.lookup().lookupClass().`package`.implementationVersion ?: "unknown"
}
