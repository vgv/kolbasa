package kolbasa.cluster.admin

import kolbasa.cluster.admin.check.MigrationState
import kolbasa.cluster.admin.check.OrphanTables
import kolbasa.cluster.admin.check.SchemaConsistency
import kolbasa.cluster.admin.check.ShardBalance
import kolbasa.cluster.admin.check.ShardMapState
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * See [ClusterAdmin.checkCluster]
 */
internal fun checkCluster(dataSources: Map<NodeId, DataSource>, checks: Set<ClusterCheck>): CheckResult {
    // A cluster without a shard map is not set up yet, whichever checks are asked for
    val nodes = ClusterHelpers.readNodes(dataSources)
    if (ClusterHelpers.readShards(nodes).mergedShards.isEmpty()) {
        throw ClusterOperationException.ShardsNotInitializedException()
    }

    var shardBalance: ShardBalanceResult? = null
    var schemaConsistency: SchemaConsistencyResult? = null
    var orphanTables: OrphanTablesResult? = null
    var migrationState: MigrationStateResult? = null
    var shardMap: ShardMapResult? = null

    for (check in checks) {
        when (check) {
            ClusterCheck.SHARD_BALANCE -> shardBalance = ShardBalance.check(dataSources)
            ClusterCheck.SCHEMA_CONSISTENCY -> schemaConsistency = SchemaConsistency.check(dataSources)
            ClusterCheck.ORPHAN_TABLES -> orphanTables = OrphanTables.check(dataSources)
            ClusterCheck.MIGRATION_STATE -> migrationState = MigrationState.check(dataSources)
            ClusterCheck.SHARD_MAP -> shardMap = ShardMapState.check(dataSources)
        }
    }

    return CheckResult(shardBalance, schemaConsistency, orphanTables, migrationState, shardMap)
}
