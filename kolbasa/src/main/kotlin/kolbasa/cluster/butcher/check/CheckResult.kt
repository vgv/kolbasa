package kolbasa.cluster.butcher.check

/**
 * What the requested checks found – one field per check, `null` for a check that was not asked for
 */
internal data class CheckResult(
    val shardBalance: ShardBalanceResult? = null,
    val schemaConsistency: SchemaConsistencyResult? = null,
    val orphanTables: OrphanTablesResult? = null,
    val migrationState: MigrationStateResult? = null
)
