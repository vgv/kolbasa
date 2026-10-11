package kolbasa.butcher.cli

import kolbasa.butcher.ShardBalanceResult
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

        val text = result.render()

        assertTrue(text.contains("Shard balance"), text)
        assertTrue(text.contains("Already balanced"), text)
        assertTrue(text.contains("342 shards"), text)
    }

    @Test
    fun testWithMoves() {
        val n1 = NodeId("n1")
        val n2 = NodeId("n2")
        val result = ShardBalanceResult(
            currentDistribution = mapOf(n1 to shards(0, 10), n2 to emptyList()),
            proposedMoves = mapOf(n2 to shards(0, 5))
        )

        val text = result.render()

        assertTrue(text.contains("Proposed moves"), text)
        assertTrue(text.contains("⟶ n2"), text)
    }

    private fun shards(from: Int, count: Int): List<ShardId> = (from until from + count).map(ShardId::of)

}
