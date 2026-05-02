package kolbasa.cluster.butcher.check

import kolbasa.cluster.butcher.MoveHelpers
import kolbasa.cluster.butcher.config.AvailableCheck
import kolbasa.cluster.butcher.config.Command

internal fun check(command: Command.Check): CheckResult {
    MoveHelpers.checkClusterNodes(command.nodes)

    val results = command.checks.map { check ->
        when (check) {
            AvailableCheck.CHECK_SHARD_BALANCE -> ShardBalance.check(command)
            AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> SchemaConsistency.check(command)
            AvailableCheck.FIND_ORPHAN_TABLES -> OrphanTables.check(command)
            AvailableCheck.CHECK_MIGRATION_STATE -> MigrationState.check(command)
        }
    }

    return CheckResult(results)
}
