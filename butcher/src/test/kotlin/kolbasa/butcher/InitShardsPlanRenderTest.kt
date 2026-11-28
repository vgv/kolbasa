package kolbasa.butcher

import kolbasa.cluster.admin.InitShardsPlan
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class InitShardsPlanRenderTest {

    private val n1 = NodeId("node-01")
    private val n2 = NodeId("node-02")
    private val n3 = NodeId("node-03")

    @Test
    fun testNothingToDo() {
        val plan = InitShardsPlan(owners = emptyMap(), unsafeNodes = emptyList())

        assertEquals("Every shard has a node already", plan.render().renderToText())
    }

    @Test
    fun testShardsPerNode() {
        // node-02 gets nothing, so it isn't listed
        val owners = mapOf(ShardId.of(5) to n3, ShardId.of(6) to n1, ShardId.of(9) to n3)
        val plan = InitShardsPlan(owners, unsafeNodes = emptyList())

        val expected = listOf(
            "node-01: gets 1 shard(s)",
            "node-03: gets 2 shard(s)",
        )
        assertEquals(expected, plan.render().renderToText().lines())
    }

    @Test
    fun testUnsafeNode() {
        val owners = mapOf(ShardId.of(0) to n1, ShardId.of(1) to n2)
        val plan = InitShardsPlan(owners, unsafeNodes = listOf(n2))

        val text = plan.render().renderToText(width = 400)
        assertTrue(text.contains("node-02 has messages, and the cluster has no shard map yet."), text)
        assertTrue(text.contains("Creating it together with other nodes is unsafe"), text)
        assertTrue(text.contains("give it every shard with --target=node-02, then rebalance the cluster."), text)
        assertTrue(!text.contains("node-01 has messages"), text)
    }
}
