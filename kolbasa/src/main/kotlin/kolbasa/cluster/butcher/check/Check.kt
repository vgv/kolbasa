package kolbasa.cluster.butcher.check

import kolbasa.cluster.butcher.MoveHelpers
import kolbasa.cluster.butcher.config.AvailableCheck
import kolbasa.cluster.butcher.config.Command

internal fun check(command: Command.Check): CheckResult {
    MoveHelpers.checkClusterNodes(command.nodes)

    var shardBalance: ShardBalanceResult? = null
    var schemaConsistency: SchemaConsistencyResult? = null
    var orphanTables: OrphanTablesResult? = null
    var migrationState: MigrationStateResult? = null

    command.checks.forEach { check ->
        when (check) {
            AvailableCheck.CHECK_SHARD_BALANCE -> shardBalance = ShardBalance.check(command)
            AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> schemaConsistency = SchemaConsistency.check(command)
            AvailableCheck.FIND_ORPHAN_TABLES -> orphanTables = OrphanTables.check(command)
            AvailableCheck.CHECK_MIGRATION_STATE -> migrationState = MigrationState.check(command)
        }
    }

    return CheckResult(
        shardBalance = shardBalance,
        schemaConsistency = schemaConsistency,
        orphanTables = orphanTables,
        migrationState = migrationState
    )
}
