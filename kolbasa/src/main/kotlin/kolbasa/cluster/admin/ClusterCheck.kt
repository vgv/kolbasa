package kolbasa.cluster.admin

/**
 * The checks of [ClusterAdmin.checkCluster]. Each of them is a method of [ClusterAdmin] as well
 */
enum class ClusterCheck {

    /**
     * How the shards are spread across the nodes, and a minimum-movement rebalance if they are uneven, see
     * [ClusterAdmin.checkShardBalance]
     */
    SHARD_BALANCE,

    /**
     * Queue tables whose structure differs between nodes, or that are missing on some of them, see
     * [ClusterAdmin.checkSchemaConsistency]
     */
    SCHEMA_CONSISTENCY,

    /**
     * DLQ and archive tables left behind on a node whose main queue table is gone, see [ClusterAdmin.checkOrphanTables]
     */
    ORPHAN_TABLES,

    /**
     * Shards that are still mid-migration, see [ClusterAdmin.checkMigrationState]
     */
    MIGRATION_STATE,

    /**
     * How the copies of the shard map on the nodes differ, and the rows that can't be right, see
     * [ClusterAdmin.checkShardMap]
     */
    SHARD_MAP,
}
