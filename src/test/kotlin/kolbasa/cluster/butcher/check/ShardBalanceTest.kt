package kolbasa.cluster.butcher.check

import kolbasa.cluster.Shard
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ShardBalanceTest {

    @Test
    fun testEmptyNodes() {
        val result = ShardBalance(shards = emptyList(), knownNodes = emptySet()).compute()

        assertTrue(result.isBalanced)
        assertEquals(0, result.totalMoves)
        assertTrue(result.currentDistribution.isEmpty())
        assertTrue(result.proposedMoves.isEmpty())
    }

    @Test
    fun testSingleNode() {
        val node = NodeId("n1")
        val shards = (0 until Shard.SHARD_COUNT).map { stableShard(it, node) }

        val result = ShardBalance(shards, setOf(node)).compute()

        assertTrue(result.isBalanced)
        assertEquals(Shard.SHARD_COUNT, result.currentDistribution[node]?.size)
    }

    @Test
    fun testAlreadyBalanced_Divisible() {
        // 4 nodes, 1024 shards -> 256 each
        val nodes = (1..4).map { NodeId("n$it") }
        val shards = (0 until Shard.SHARD_COUNT).map { stableShard(it, nodes[it % 4]) }

        val result = ShardBalance(shards, nodes.toSet()).compute()

        assertTrue(result.isBalanced)
        nodes.forEach { node ->
            assertEquals(256, result.currentDistribution[node]?.size)
        }
    }

    @Test
    fun testAlreadyBalanced_NonDivisible() {
        // 3 nodes, 1024 shards -> 342/341/341
        val nodes = listOf(NodeId("n1"), NodeId("n2"), NodeId("n3"))
        val counts = mapOf(nodes[0] to 342, nodes[1] to 341, nodes[2] to 341)
        val shards = buildBalancedShards(counts)

        val result = ShardBalance(shards, nodes.toSet()).compute()

        assertTrue(result.isBalanced, "Expected balanced, got $result")
    }

    @Test
    fun testSkewed_OneNodeHasAll() {
        // n1 has 1024, n2/n3/n4 have 0. Target 256/256/256/256 -> 768 moves.
        val nodes = (1..4).map { NodeId("n$it") }
        val shards = (0 until Shard.SHARD_COUNT).map { stableShard(it, nodes[0]) }

        val result = ShardBalance(shards, nodes.toSet()).compute()

        assertFalse(result.isBalanced)
        assertEquals(768, result.totalMoves)
        assertEquals(256, result.currentDistribution[nodes[0]]?.size?.let { it - (result.proposedMoves.values.flatten().count { s -> s.producerNode == nodes[0] }) })
        // After applying moves, each recipient gets 256; only one source (n1).
        assertEquals(256, result.proposedMoves[nodes[1]]?.size)
        assertEquals(256, result.proposedMoves[nodes[2]]?.size)
        assertEquals(256, result.proposedMoves[nodes[3]]?.size)
        // Sources are all n1
        result.proposedMoves.values.flatten().forEach { shard ->
            assertEquals(nodes[0], shard.producerNode)
        }
    }

    @Test
    fun testSkewed_NonDivisible() {
        // 3 nodes, 1024 shards on n1. Target 342/341/341. Surplus from n1 = 1024-342 = 682.
        val nodes = (1..3).map { NodeId("n$it") }
        val shards = (0 until Shard.SHARD_COUNT).map { stableShard(it, nodes[0]) }

        val result = ShardBalance(shards, nodes.toSet()).compute()

        assertEquals(682, result.totalMoves)
        assertEquals(341, result.proposedMoves[nodes[1]]?.size)
        assertEquals(341, result.proposedMoves[nodes[2]]?.size)
    }

    @Test
    fun testCeilSlotsAwardedToLargestNodes_MinimizesMoves() {
        // 3 nodes, 1024 shards: n1=400, n2=400, n3=224
        // Target ceil=342, floor=341. Giving ceil (342) to the two largest (400s)
        // requires moving 58+58 = 116 off them, and n3 needs 341-224 = 117. Total 117 moves.
        // (The extra 1 balances since surplus is 58+59 = 117.)
        val nodes = listOf(NodeId("n1"), NodeId("n2"), NodeId("n3"))
        val counts = mapOf(nodes[0] to 400, nodes[1] to 400, nodes[2] to 224)
        val shards = buildBalancedShards(counts)

        val result = ShardBalance(shards, nodes.toSet()).compute()

        assertEquals(117, result.totalMoves)
        // All moves land on n3
        assertEquals(117, result.proposedMoves[nodes[2]]?.size)
    }

    @Test
    fun testMigrationStateShardCountedAtTargetNode() {
        // A migrating shard: producerNode = nextConsumerNode = n2, consumerNode = null.
        // The balancer should count it toward n2 (not the original source).
        val n1 = NodeId("n1")
        val n2 = NodeId("n2")
        val stableOnN1 = (0 until 512).map { stableShard(it, n1) }
        val migratingToN2 = (512 until 1024).map { migratingShard(it, n2) }

        val result = ShardBalance(stableOnN1 + migratingToN2, setOf(n1, n2)).compute()

        assertTrue(result.isBalanced)
        assertEquals(512, result.currentDistribution[n1]?.size)
        assertEquals(512, result.currentDistribution[n2]?.size)
    }

    @Test
    fun testOrphanShards_Skipped() {
        // Some shards point to an unknown node. Balancer should silently exclude them.
        val n1 = NodeId("n1")
        val ghost = NodeId("n-gone")
        val onN1 = (0 until 900).map { stableShard(it, n1) }
        val orphans = (900 until 1024).map { stableShard(it, ghost) }

        val result = ShardBalance(onN1 + orphans, setOf(n1)).compute()

        // Only n1 is in the distribution. Orphans are excluded entirely.
        assertTrue(result.isBalanced)
        assertEquals(900, result.currentDistribution[n1]?.size)
        assertEquals(1, result.currentDistribution.size)
    }

    @Test
    fun testUnderloaded_ZeroShardCluster() {
        // Edge: no shards in the cluster yet. N=3, target 0/0/0.
        val nodes = (1..3).map { NodeId("n$it") }

        val result = ShardBalance(emptyList(), nodes.toSet()).compute()

        assertTrue(result.isBalanced)
        nodes.forEach { node ->
            assertEquals(0, result.currentDistribution[node]?.size)
        }
    }

    @Test
    fun testToString_Balanced() {
        val nodes = (1..3).map { NodeId("n$it") }
        val counts = mapOf(nodes[0] to 342, nodes[1] to 341, nodes[2] to 341)
        val shards = buildBalancedShards(counts)

        val text = ShardBalance(shards, nodes.toSet()).compute().toString()

        assertTrue(text.contains("Shard balance"), text)
        assertTrue(text.contains("Already balanced"), text)
        assertTrue(text.contains("342 shards"), text)
    }

    @Test
    fun testToString_WithMoves() {
        val nodes = (1..2).map { NodeId("n$it") }
        val shards = (0 until 10).map { stableShard(it, nodes[0]) }

        val text = ShardBalance(shards, nodes.toSet()).compute().toString()

        assertTrue(text.contains("Proposed moves"), text)
        assertTrue(text.contains("n1 -> n2"), text)
    }

    @Test
    fun testDeterminism_SameInputProducesSameOutput() {
        val nodes = (1..4).map { NodeId("n$it") }
        val shards = (0 until Shard.SHARD_COUNT).map { stableShard(it, nodes[it % 2]) }

        val first = ShardBalance(shards, nodes.toSet()).compute()
        val second = ShardBalance(shards, nodes.toSet()).compute()

        assertEquals(first.totalMoves, second.totalMoves)
        assertEquals(
            first.proposedMoves.mapValues { (_, list) -> list.map { it.shard } },
            second.proposedMoves.mapValues { (_, list) -> list.map { it.shard } }
        )
    }

    @Test
    fun testMovedShardsCountMatchesPostConditionBalance() {
        // After applying proposedMoves, each node should hold floor or ceil shards.
        val nodes = (1..5).map { NodeId("n$it") }
        // Skew: put 1024 shards on n1 alone
        val shards = (0 until Shard.SHARD_COUNT).map { stableShard(it, nodes[0]) }

        val result = ShardBalance(shards, nodes.toSet()).compute()

        val floor = Shard.SHARD_COUNT / nodes.size
        val ceil = floor + if (Shard.SHARD_COUNT % nodes.size == 0) 0 else 1
        val final = nodes.associateWith { node ->
            (result.currentDistribution[node]?.size ?: 0) -
                result.proposedMoves.values.flatten().count { it.producerNode == node } +
                (result.proposedMoves[node]?.size ?: 0)
        }

        final.forEach { (node, count) ->
            assertTrue(count == floor || count == ceil, "Node $node has $count shards; expected $floor or $ceil")
        }
    }

    // ---------- helpers ----------

    private fun stableShard(shardNum: Int, node: NodeId): Shard =
        Shard(shard = shardNum, producerNode = node, consumerNode = node, nextConsumerNode = null)

    private fun migratingShard(shardNum: Int, target: NodeId): Shard =
        Shard(shard = shardNum, producerNode = target, consumerNode = null, nextConsumerNode = target)

    /**
     * Build [counts.values.sum()] distinct stable shards distributed per the given counts.
     */
    private fun buildBalancedShards(counts: Map<NodeId, Int>): List<Shard> {
        val result = mutableListOf<Shard>()
        var next = 0
        counts.forEach { (node, count) ->
            repeat(count) {
                result += stableShard(next++, node)
            }
        }
        return result
    }
}
