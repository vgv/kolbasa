package kolbasa.cluster.butcher.config

/**
 * Available cluster checks that can be run independently or all at once.
 *
 * @param checkName short CLI name used in `check-cluster <subcheck> <config-file>`
 */
internal enum class AvailableCheck(val checkName: String, val shortUsage: String) {

    /**
     * Check shard balancing, advise about possible migration to distribute load evenly
     */
    CHECK_SHARD_BALANCE(
        checkName = "shard-balance",
        shortUsage = "Check shard balancing, advise about possible migration to distribute load evenly"
    ),

    /**
     * Check that all queue tables exist on every node with the same structure (columns, indexes)
     */
    CHECK_SCHEMA_CONSISTENCY(
        checkName = "schema-consistency",
        shortUsage = "Check that all queue tables exist on every node with the same structure (columns, indexes)"
    ),

    /**
     * Find orphan companion tables (DLQ and Archive) on some nodes but not others
     */
    FIND_ORPHAN_TABLES(
        checkName = "find-orphans",
        shortUsage = "Find orphan companion tables (DLQ and Archive) on some nodes but not others"
    ),

    /**
     * Find shards stuck in migration state (consumerNode == null, nextConsumerNode != null)
     */
    CHECK_MIGRATION_STATE(
        checkName = "migration-state",
        shortUsage = "Find shards stuck in migration state"
    );

    fun usageLine(): String = AvailableCheck.usageLine(checkName, shortUsage)

    companion object {
        init {
            val duplicates = entries.groupBy { it.checkName }.filterValues { it.size > 1 }.keys
            check(duplicates.isEmpty()) {
                "Duplicate check names in AvailableCheck: $duplicates"
            }
        }

        val all: Set<AvailableCheck> = entries.toSet()

        fun fromCheckName(name: String): AvailableCheck? = entries.find { it.checkName == name }

        fun usageLine(name: String, usage: String): String {
            val defaultPadding = AvailableCheck.entries.maxOf { it.checkName.length } + 2
            return "${name.padEnd(defaultPadding)}$usage"
        }
    }
}
