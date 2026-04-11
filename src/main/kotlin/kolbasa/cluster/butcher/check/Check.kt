package kolbasa.cluster.butcher.check

import kolbasa.cluster.butcher.MoveHelpers
import kolbasa.cluster.butcher.config.AvailableCheck
import kolbasa.cluster.butcher.config.Command

internal fun check(command: Command.Check): CheckResult {
    MoveHelpers.checkClusterNodes(command.nodes)

    val results = command.checks.mapNotNull { check ->
        when (check) {
            AvailableCheck.CHECK_SHARD_BALANCE -> ShardBalance.check(command)
            AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> checkSchemaConsistency(command)
            AvailableCheck.FIND_ORPHAN_TABLES -> findOrphanTables(command)
            AvailableCheck.CHECK_MIGRATION_STATE -> MigrationState.check(command)
        }
    }

    return CheckResult(results)
}

private fun checkSchemaConsistency(command: Command.Check): Any? {
    // TODO: check schema consistency
    return null
}

private fun findOrphanTables(command: Command.Check): Any? {
    // TODO: find orphan tables
    return null
}

