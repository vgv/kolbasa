package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.ProgramResult
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.split
import com.github.ajalt.clikt.parameters.options.unique
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import kolbasa.butcher.HelpSection
import kolbasa.butcher.render
import kolbasa.cluster.admin.ClusterCheck

internal class CheckCluster : ClusterCommand(name = "check-cluster") {

    private val checks: Set<ClusterCheck> by option("--checks", metavar = "<check,check,...>")
        .enum<ClusterCheck> { check -> check.cliName() }
        .split(",")
        .default(ClusterCheck.entries)
        .unique()
        .help("Which checks to run, comma-separated. All of them by default")

    override val helpSection = HelpSection.DIAGNOSTICS

    // It prints the report and a summary line itself, see execute()
    override val reportsCompletion = false

    override fun help(context: Context) =
        "Inspect cluster state (schema consistency, shards distribution etc.). Changes nothing, safe at any time"

    override fun helpEpilog(context: Context) =
        "Exits 0 when every check passed, 2 when a check found something, and 1 when the checks could not run.\n\n" +
            super.helpEpilog(context)

    /**
     * Prints the report and a summary line, and exits 2 if a check found something
     *
     * The exit code is set by an exception, so the output can't wait for [run]: it is printed here.
     */
    override fun execute(): Widget? {
        val result = butcher().checkCluster(checks)
        terminal.println(result.render())

        if (result.isClean) {
            echo(TextColors.green("All checks passed."))
            return null
        }

        val names = result.findings.joinToString(", ") { check -> check.cliName() }
        echo(TextColors.yellow("${result.findings.size} of ${result.checks.size} check(s) found something: $names."))

        throw ProgramResult(EXIT_FOUND_SOMETHING)
    }
}

/**
 * How a check is named on the command line, in `--checks`
 */
internal fun ClusterCheck.cliName(): String = when (this) {
    ClusterCheck.SHARD_BALANCE -> "shard-balance"
    ClusterCheck.SCHEMA_CONSISTENCY -> "schema-consistency"
    ClusterCheck.ORPHAN_TABLES -> "orphan-tables"
    ClusterCheck.MIGRATION_STATE -> "migration-state"
    ClusterCheck.SHARD_MAP -> "shard-map"
}

/**
 * The exit code of check-cluster when the checks ran and one found something. 1 is taken: the checks could not run
 */
private const val EXIT_FOUND_SOMETHING = 2
