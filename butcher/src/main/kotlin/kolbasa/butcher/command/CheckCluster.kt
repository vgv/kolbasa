package kolbasa.butcher.command

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.ProgramResult
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.split
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.rendering.Widget
import kolbasa.butcher.HelpSection
import kolbasa.butcher.config.AvailableCheck
import kolbasa.butcher.render
import kolbasa.cluster.admin.CheckResult
import kolbasa.cluster.admin.MigrationStateResult
import kolbasa.cluster.admin.OrphanTablesResult
import kolbasa.cluster.admin.SchemaConsistencyResult
import kolbasa.cluster.admin.ShardBalanceResult
import kolbasa.cluster.admin.ShardMapResult

internal class CheckCluster : ClusterCommand(name = "check-cluster") {

    private val checks: List<AvailableCheck> by option("--checks", metavar = "<check,check,...>")
        .enum<AvailableCheck> { it.checkName }
        .split(",")
        .default(AvailableCheck.entries)
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
        val butcher = butcher()

        var shardBalance: ShardBalanceResult? = null
        var schemaConsistency: SchemaConsistencyResult? = null
        var orphanTables: OrphanTablesResult? = null
        var migrationState: MigrationStateResult? = null
        var shardMap: ShardMapResult? = null

        // Which checks exist and how they are named on the command line is a property of the CLI, so the
        // mapping lives here and not in the library: every check is callable on its own
        checks.forEach { check ->
            when (check) {
                AvailableCheck.CHECK_SHARD_BALANCE -> shardBalance = butcher.checkShardBalance()
                AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> schemaConsistency = butcher.checkSchemaConsistency()
                AvailableCheck.CHECK_ORPHAN_TABLES -> orphanTables = butcher.checkOrphanTables()
                AvailableCheck.CHECK_MIGRATION_STATE -> migrationState = butcher.checkMigrationState()
                AvailableCheck.CHECK_SHARD_MAP -> shardMap = butcher.checkShardMap()
            }
        }

        val result = CheckResult(
            shardBalance = shardBalance,
            schemaConsistency = schemaConsistency,
            orphanTables = orphanTables,
            migrationState = migrationState,
            shardMap = shardMap
        )
        terminal.println(result.render())

        if (result.isClean) {
            echo(TextColors.green("All checks passed."))
            return null
        }

        // The checks that found something, by the names they were asked for with
        val askedChecks = checks.distinct()
        val checksWithFindings = askedChecks.filterNot { check -> isClean(result, check) }
        val names = checksWithFindings.joinToString(", ") { check -> check.checkName }
        echo(TextColors.yellow("${checksWithFindings.size} of ${askedChecks.size} check(s) found something: $names."))

        throw ProgramResult(EXIT_FOUND_SOMETHING)
    }

    private fun isClean(result: CheckResult, check: AvailableCheck): Boolean {
        val clean = when (check) {
            AvailableCheck.CHECK_SHARD_BALANCE -> result.shardBalance?.isClean
            AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> result.schemaConsistency?.isClean
            AvailableCheck.CHECK_ORPHAN_TABLES -> result.orphanTables?.isClean
            AvailableCheck.CHECK_MIGRATION_STATE -> result.migrationState?.isClean
            AvailableCheck.CHECK_SHARD_MAP -> result.shardMap?.isClean
        }

        // A check that was asked for always has a result
        return clean != false
    }
}

/**
 * The exit code of check-cluster when the checks ran and one found something. 1 is taken: the checks could not run
 */
private const val EXIT_FOUND_SOMETHING = 2
