package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CheckResultTest {

    private val node = NodeId("node-01")

    @Test
    fun testNoChecks_IsClean() {
        val result = CheckResult()

        assertTrue(result.isClean)
        assertEquals(emptyList<ClusterCheck>(), result.checks)
        assertEquals(emptyList<ClusterCheck>(), result.findings)
    }

    @Test
    fun testChecksAndFindings() {
        val result = CheckResult(
            shardBalance = ShardBalanceResult(mapOf(node to emptyList()), emptyMap()),
            orphanTables = OrphanTablesResult(emptyMap()),
            migrationState = MigrationStateResult(mapOf(node to listOf(ShardId.of(5))))
        )

        // In the order of ClusterCheck, whatever the order of the fields
        assertEquals(
            listOf(ClusterCheck.SHARD_BALANCE, ClusterCheck.ORPHAN_TABLES, ClusterCheck.MIGRATION_STATE),
            result.checks
        )
        assertEquals(listOf(ClusterCheck.SHARD_BALANCE, ClusterCheck.MIGRATION_STATE), result.findings)
        assertFalse(result.isClean)
    }

    @Test
    fun testAllChecksClean_IsClean() {
        val result = CheckResult(
            shardBalance = ShardBalanceResult(mapOf(node to listOf(ShardId.of(0))), emptyMap()),
            orphanTables = OrphanTablesResult(emptyMap()),
            migrationState = MigrationStateResult(emptyMap())
        )

        assertTrue(result.isClean)
    }

    @Test
    fun testOneCheckFoundSomething_IsNotClean() {
        val result = CheckResult(
            orphanTables = OrphanTablesResult(emptyMap()),
            migrationState = MigrationStateResult(mapOf(node to listOf(ShardId.of(5))))
        )

        assertFalse(result.isClean)
    }

    @Test
    fun testNoShardBelongsToANode_IsNotClean() {
        // Nothing can move, so the report is balanced, but it is not clean: the report shows this in yellow
        val shardBalance = ShardBalanceResult(mapOf(node to emptyList()), emptyMap())

        assertTrue(shardBalance.isBalanced)
        assertFalse(shardBalance.isClean)
        assertFalse(CheckResult(shardBalance = shardBalance).isClean)
    }
}
