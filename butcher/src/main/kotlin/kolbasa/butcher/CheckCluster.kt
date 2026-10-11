package kolbasa.butcher

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.split
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.mordant.rendering.Widget
import kolbasa.butcher.config.AvailableCheck
import kolbasa.cluster.admin.CheckResult
import kolbasa.cluster.admin.MigrationStateResult
import kolbasa.cluster.admin.OrphanTablesResult
import kolbasa.cluster.admin.SchemaConsistencyResult
import kolbasa.cluster.admin.ShardBalanceResult

internal class CheckCluster : ClusterCommand(name = "check-cluster") {

    private val checks by option("--checks", metavar = "<check,check,...>")
        .enum<AvailableCheck> { it.checkName }
        .split(",")
        .default(AvailableCheck.all.toList())
        .help("Which checks to run, comma-separated. All of them by default")

    override fun help(context: Context) =
        "Inspect cluster state (schema consistency, shards distribution etc.). Modifies nothing, safe at any time"

    override fun execute(): Widget {
        val butcher = butcher()
        butcher.checkNodesAvailable()

        var shardBalance: ShardBalanceResult? = null
        var schemaConsistency: SchemaConsistencyResult? = null
        var orphanTables: OrphanTablesResult? = null
        var migrationState: MigrationStateResult? = null

        // Which checks exist and how they are named on the command line is a property of the CLI, so the
        // mapping lives here and not in the library: every check is callable on its own
        checks.forEach { check ->
            when (check) {
                AvailableCheck.CHECK_SHARD_BALANCE -> shardBalance = butcher.checkShardBalance()
                AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> schemaConsistency = butcher.checkSchemaConsistency()
                AvailableCheck.FIND_ORPHAN_TABLES -> orphanTables = butcher.findOrphanTables()
                AvailableCheck.CHECK_MIGRATION_STATE -> migrationState = butcher.checkMigrationState()
            }
        }

        return CheckResult(
            shardBalance = shardBalance,
            schemaConsistency = schemaConsistency,
            orphanTables = orphanTables,
            migrationState = migrationState
        ).render()
    }
}
