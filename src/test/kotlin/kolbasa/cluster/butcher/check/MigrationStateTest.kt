package kolbasa.cluster.butcher.check

import kolbasa.cluster.Shard
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MigrationStateTest {

    @Test
    fun testAllStable_ReturnsClean() {
        val n1 = NodeId("n1")
        val shards = (0 until 10).map { stableShard(it, n1) }

        val result = MigrationState(shards).compute()

        assertTrue(result.isClean)
        assertEquals(0, result.totalMigratingShards)
        assertTrue(result.migratingShardsByTarget.isEmpty())
    }

    @Test
    fun testEmptyInput() {
        val result = MigrationState(emptyList()).compute()

        assertTrue(result.isClean)
        assertEquals(0, result.totalMigratingShards)
    }

    @Test
    fun testSingleMigratingShard_GroupedByTarget() {
        val target = NodeId("n2")
        val result = MigrationState(listOf(migratingShard(42, target))).compute()

        assertFalse(result.isClean)
        assertEquals(1, result.totalMigratingShards)
        assertEquals(listOf(42), result.migratingShardsByTarget[target]?.map { it.shard })
    }

    @Test
    fun testMixed_OnlyMigratingReturned() {
        val n1 = NodeId("n1")
        val n2 = NodeId("n2")
        val shards = listOf(
            stableShard(0, n1),
            stableShard(1, n1),
            migratingShard(2, n2),
            stableShard(3, n1),
            migratingShard(4, n2),
        )

        val result = MigrationState(shards).compute()

        assertEquals(2, result.totalMigratingShards)
        assertEquals(listOf(2, 4), result.migratingShardsByTarget[n2]?.map { it.shard })
    }

    @Test
    fun testMultipleTargets_GroupedCorrectly() {
        val n2 = NodeId("n2")
        val n3 = NodeId("n3")
        val shards = listOf(
            migratingShard(10, n2),
            migratingShard(20, n3),
            migratingShard(30, n2),
            migratingShard(40, n3),
        )

        val result = MigrationState(shards).compute()

        assertEquals(4, result.totalMigratingShards)
        assertEquals(listOf(10, 30), result.migratingShardsByTarget[n2]?.map { it.shard }?.sorted())
        assertEquals(listOf(20, 40), result.migratingShardsByTarget[n3]?.map { it.shard }?.sorted())
    }

    @Test
    fun testToString_Clean() {
        val result = MigrationState(emptyList()).compute()

        assertEquals("Migration state: no shards in migration", result.toString())
    }

    @Test
    fun testToString_WithMigrations_ContainsShardAndTarget() {
        val n2 = NodeId("n2")
        val n3 = NodeId("n3")
        val shards = listOf(
            migratingShard(30, n3),
            migratingShard(10, n2),
            migratingShard(20, n2),
        )

        val text = MigrationState(shards).compute().toString()

        assertTrue(text.contains("3 shard(s) in migration"), text)
        assertTrue(text.contains("-> n2"), text)
        assertTrue(text.contains("-> n3"), text)
        // Targets sorted by id (n2 before n3) and shards sorted ascending within each group.
        val n2Pos = text.indexOf("-> n2")
        val n3Pos = text.indexOf("-> n3")
        assertTrue(n2Pos < n3Pos, "n2 group should come before n3 group:\n$text")
        val shard10Pos = text.indexOf("shard   10")
        val shard20Pos = text.indexOf("shard   20")
        assertTrue(shard10Pos in 0 until shard20Pos, "shard 10 should appear before shard 20:\n$text")
    }

    // ---------- helpers ----------

    private fun stableShard(shardNum: Int, node: NodeId): Shard =
        Shard(shard = shardNum, producerNode = node, consumerNode = node, nextConsumerNode = null)

    private fun migratingShard(shardNum: Int, target: NodeId): Shard =
        Shard(shard = shardNum, producerNode = target, consumerNode = null, nextConsumerNode = target)
}
