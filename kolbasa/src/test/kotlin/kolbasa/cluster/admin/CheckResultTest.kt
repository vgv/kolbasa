package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CheckResultTest {

    private val node = NodeId("node-01")

    @Test
    fun testNoChecks_IsClean() {
        assertTrue(CheckResult().isClean)
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
