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
     * True when none of the checks that were asked for found anything
     */
    val isClean: Boolean = listOfNotNull(
        shardBalance?.isClean,
        schemaConsistency?.isClean,
        orphanTables?.isClean,
        migrationState?.isClean,
        shardMap?.isClean
    ).all { clean -> clean }
}
