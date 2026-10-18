package kolbasa.butcher

import kolbasa.cluster.admin.ShardBalanceResult
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ShardBalanceResultRenderTest {

    @Test
    fun testBalanced() {
        val nodes = (1..3).map { NodeId("n$it") }
        val result = ShardBalanceResult(
            currentDistribution = mapOf(
                nodes[0] to shards(0, 342),
                nodes[1] to shards(342, 341),
                nodes[2] to shards(683, 341),
            ),
            proposedMoves = emptyMap()
        )

        val text = result.render().renderToText()

        assertTrue(text.contains("1024 shard(s) evenly spread across 3 node(s)"), text)
        nodes.forEach { assertTrue(text.contains(it.id), text) }
        // Nothing to move, so nothing to paste into prepare-migration and nothing to change
        assertTrue(!text.contains("--shards="), text)
        assertTrue(!text.contains("Change"), text)
    }

    @Test
    fun testWithMoves() {
        val n1 = NodeId("n1")
        val n2 = NodeId("n2")
        val result = ShardBalanceResult(
            currentDistribution = mapOf(n1 to shards(0, 10), n2 to emptyList()),
            proposedMoves = mapOf(n2 to shards(0, 5))
        )

        val text = result.render().renderToText()

        assertTrue(text.contains("5 should move"), text)
        assertTrue(text.contains("Proposed moves (5)"), text)
        // What each node gains or loses, as a column of its own
        assertTrue(text.contains("Change"), text)
        assertTrue(text.contains("-5"), text)
        assertTrue(text.contains("+5"), text)
        // The plan is printed as the flags it is about to be pasted into
        assertTrue(text.contains("--target=n2 --shards=0,1,2,3,4"), text)
    }

    @Test
    fun testNodeDrainPlan() {
        val nodes = (1..3).map { NodeId("n$it") }
        val result = ShardBalanceResult(
            currentDistribution = mapOf(
                nodes[0] to shards(0, 342),
                nodes[1] to shards(342, 341),
                nodes[2] to shards(683, 341),
            ),
            // A drain proposes the leaving node's shards and nothing else
            proposedMoves = mapOf(nodes[0] to shards(683, 170), nodes[1] to shards(853, 171))
        )

        val text = result.renderNodesDrain(setOf(nodes[2])).renderToText()

        assertTrue(text.contains("Draining n3: 341 shard(s) to move onto 2 node(s)"), text)
        assertTrue(text.contains("Change"), text)
        assertTrue(text.contains("-341"), text)
        // No flag to paste the shards into: a drain is named by its node, not by its shards
        assertTrue(!text.contains("--shards="), text)
        assertTrue(!text.contains("--target="), text)
    }

    @Test
    fun testDrainedNodeOwnsNothing() {
        val nodes = (1..3).map { NodeId("n$it") }
        val result = ShardBalanceResult(
            currentDistribution = mapOf(
                nodes[0] to shards(0, 512),
                nodes[1] to shards(512, 512),
                nodes[2] to emptyList(),
            ),
            proposedMoves = emptyMap()
        )

        val text = result.renderNodesDrain(setOf(nodes[2])).renderToText()

        assertTrue(text.contains("Node n3 owns no shards"), text)
        // 0/512/512 is not "evenly spread", so the balance headline must not appear
        assertTrue(!text.contains("evenly spread"), text)
        assertTrue(!text.contains("Change"), text)
    }

    @Test
    fun testSeveralDrainedNodesOwnNothing() {
        val nodes = (1..3).map { NodeId("n$it") }
        val result = ShardBalanceResult(
            currentDistribution = mapOf(
                nodes[0] to shards(0, 1024),
                nodes[1] to emptyList(),
                nodes[2] to emptyList(),
            ),
            proposedMoves = emptyMap()
        )

        val text = result.renderNodesDrain(setOf(nodes[2], nodes[1])).renderToText()

        // Nodes in a fixed order, whatever order they were typed in
        assertTrue(text.contains("Nodes n2, n3 own no shards"), text)
    }

    private fun shards(from: Int, count: Int): List<ShardId> = (from until from + count).map(ShardId::of)

}
