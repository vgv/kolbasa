package kolbasa.butcher

import kolbasa.cluster.admin.InitClusterPlan
import kolbasa.cluster.admin.InitNodesPlan
import kolbasa.cluster.admin.InitShardsPlan
import kolbasa.cluster.admin.NewNode
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class InitClusterPlanRenderTest {

    private val n1 = NodeId("node-01")
    private val n2 = NodeId("node-02")
    private val n3 = NodeId("node-03")

    @Test
    fun testNothingToDo() {
        val plan = InitClusterPlan(nodesPlan(emptyList(), listOf(n1, n2)), InitShardsPlan(emptyMap(), emptyList()), 0)

        assertEquals("The cluster is initialized already", plan.render().renderToText())
    }

    @Test
    fun testNewNodeJoins() {
        val nodes = nodesPlan(listOf(NewNode(n3, 2, hasMessages = false)), listOf(n1, n2))
        val plan = InitClusterPlan(nodes, InitShardsPlan(emptyMap(), emptyList()), copiesToSync = 1)

        val expected = listOf(
            "node-03: gets its id, with bucket 2",
            "1 of 3 copies of the shard map will be brought up to date",
        )
        assertEquals(expected, plan.render().renderToText().lines())
    }

    @Test
    fun testNewCluster() {
        val nodes = nodesPlan(listOf(NewNode(n1, 0, hasMessages = false), NewNode(n2, 1, hasMessages = false)), emptyList())
        val owners = mapOf(ShardId.of(0) to n1, ShardId.of(1) to n2, ShardId.of(2) to n1)
        val plan = InitClusterPlan(nodes, InitShardsPlan(owners, emptyList()), copiesToSync = 0)

        val expected = listOf(
            "node-01: gets its id, with bucket 0",
            "node-02: gets its id, with bucket 1",
            "node-01: gets 2 shard(s)",
            "node-02: gets 1 shard(s)",
        )
        assertEquals(expected, plan.render().renderToText().lines())
    }

    @Test
    fun testUnsafeNodes() {
        // node-01 has messages and an id already, node-02 has messages and no id: both on a cluster without a shard map
        val nodes = InitNodesPlan(
            newNodes = listOf(NewNode(n2, 1, hasMessages = true)),
            initializedNodes = listOf(n1),
            unsafeNodes = listOf(n2)
        )
        val owners = mapOf(ShardId.of(0) to n1, ShardId.of(1) to n2)
        val plan = InitClusterPlan(nodes, InitShardsPlan(owners, unsafeNodes = listOf(n1, n2)), copiesToSync = 0)

        val text = plan.render().renderToText(width = 400)
        val lines = text.lines().filter { it.contains("has messages") }
        assertEquals(2, lines.size, text)
        assertTrue(lines[0].startsWith("node-01 has messages, and the cluster has no shard map yet."), text)
        assertTrue(!lines[0].contains("message ids"), text)
        assertTrue(lines[1].startsWith("node-02 has messages, and the cluster has no shard map yet."), text)
        assertTrue(lines[1].contains("Its message ids can collide with another node's as well."), text)
        assertTrue(lines[1].endsWith("Initialize the cluster with this node alone first, then add the other nodes."), text)
    }

    @Test
    fun testNodeWithMessagesJoins() {
        // The cluster has a shard map: the new node's shards all belong to the other nodes
        val nodes = InitNodesPlan(
            newNodes = listOf(NewNode(n3, 2, hasMessages = true)),
            initializedNodes = listOf(n1, n2),
            unsafeNodes = listOf(n3)
        )
        val plan = InitClusterPlan(nodes, InitShardsPlan(emptyMap(), emptyList()), copiesToSync = 1)

        val text = plan.render().renderToText(width = 400)
        assertTrue(text.contains("node-03 has messages but no id yet. Adding it to this cluster is unsafe"), text)
        assertTrue(!text.contains("alone first"), text)
    }

    private fun nodesPlan(newNodes: List<NewNode>, initializedNodes: List<NodeId>) =
        InitNodesPlan(newNodes, initializedNodes, unsafeNodes = emptyList())
}
