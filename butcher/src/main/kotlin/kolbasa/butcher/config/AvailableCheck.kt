package kolbasa.butcher.config

/**
 * Available cluster checks that can be run independently or all at once.
 *
 * @param checkName short CLI name used in `check-cluster <subcheck> <config-file>`
 */
internal enum class AvailableCheck(val checkName: String) {

    /**
     * Check shard balancing, advise about possible migration to distribute load evenly
     */
    CHECK_SHARD_BALANCE(checkName = "shard-balance"),

    /**
     * Check that all queue tables exist on every node with the same structure (columns, indexes)
     */
    CHECK_SCHEMA_CONSISTENCY(checkName = "schema-consistency"),

    /**
     * Find orphan companion tables (DLQ and Archive) on some nodes but not others
     */
    FIND_ORPHAN_TABLES(checkName = "find-orphans"),

    /**
     * Find shards stuck in migration state (consumerNode == null, nextConsumerNode != null)
     */
    CHECK_MIGRATION_STATE(checkName = "migration-state");

    companion object {
        init {
            val duplicates = entries.groupBy { it.checkName }.filterValues { it.size > 1 }.keys
            check(duplicates.isEmpty()) {
                "Duplicate check names in AvailableCheck: $duplicates"
            }
        }
    }
}
