package kolbasa.cluster.admin

/**
 * What the requested checks found – one field per check, `null` for a check that was not asked for
 */
data class CheckResult(
    val shardBalance: ShardBalanceResult? = null,
    val schemaConsistency: SchemaConsistencyResult? = null,
    val orphanTables: OrphanTablesResult? = null,
    val migrationState: MigrationStateResult? = null,
    val shardMap: ShardMapResult? = null
) {

    /**
     * The checks that were asked for: the ones with a result, in the order of [ClusterCheck]
     */
    val checks: List<ClusterCheck>

    /**
     * The checks of [checks] that found something, in the order of [ClusterCheck]
     */
    val findings: List<ClusterCheck>

    init {
        val checks = mutableListOf<ClusterCheck>()
        val findings = mutableListOf<ClusterCheck>()

        if (shardBalance != null) {
            checks += ClusterCheck.SHARD_BALANCE
            if (!shardBalance.isClean) findings += ClusterCheck.SHARD_BALANCE
        }
        if (schemaConsistency != null) {
            checks += ClusterCheck.SCHEMA_CONSISTENCY
            if (!schemaConsistency.isClean) findings += ClusterCheck.SCHEMA_CONSISTENCY
        }
        if (orphanTables != null) {
            checks += ClusterCheck.ORPHAN_TABLES
            if (!orphanTables.isClean) findings += ClusterCheck.ORPHAN_TABLES
        }
        if (migrationState != null) {
            checks += ClusterCheck.MIGRATION_STATE
            if (!migrationState.isClean) findings += ClusterCheck.MIGRATION_STATE
        }
        if (shardMap != null) {
            checks += ClusterCheck.SHARD_MAP
            if (!shardMap.isClean) findings += ClusterCheck.SHARD_MAP
        }

        this.checks = checks
        this.findings = findings
    }

    /**
     * True when none of the checks that were asked for found anything
     */
    val isClean: Boolean = findings.isEmpty()
}
