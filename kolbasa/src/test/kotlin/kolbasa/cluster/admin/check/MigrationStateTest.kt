package kolbasa.cluster.admin.check

import kolbasa.cluster.Shard
import kolbasa.producer.ShardId
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
        assertEquals(listOf(42), result.migratingShardsByTarget[target]?.map { it.id })
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
        assertEquals(listOf(2, 4), result.migratingShardsByTarget[n2]?.map { it.id })
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
        assertEquals(listOf(10, 30), result.migratingShardsByTarget[n2]?.map { it.id }?.sorted())
        assertEquals(listOf(20, 40), result.migratingShardsByTarget[n3]?.map { it.id }?.sorted())
    }

    // ---------- helpers ----------

    private fun stableShard(shardNum: Int, node: NodeId): Shard =
        Shard(shard = ShardId.of(shardNum), producerNode = node, consumerNode = node, nextConsumerNode = null)

    private fun migratingShard(shardNum: Int, target: NodeId): Shard =
        Shard(shard = ShardId.of(shardNum), producerNode = target, consumerNode = null, nextConsumerNode = target)
}
