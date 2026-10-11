package kolbasa.cluster.admin

/**
 * What the requested checks found – one field per check, `null` for a check that was not asked for
 */
data class CheckResult(
    val shardBalance: ShardBalanceResult? = null,
    val schemaConsistency: SchemaConsistencyResult? = null,
    val orphanTables: OrphanTablesResult? = null,
    val migrationState: MigrationStateResult? = null
)
