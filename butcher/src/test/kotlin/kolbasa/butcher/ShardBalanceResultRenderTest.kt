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

    private fun shards(from: Int, count: Int): List<ShardId> = (from until from + count).map(ShardId::of)

}
