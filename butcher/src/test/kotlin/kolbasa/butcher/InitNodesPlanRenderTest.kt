package kolbasa.butcher

import kolbasa.cluster.admin.InitNodesPlan
import kolbasa.cluster.admin.NewNode
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class InitNodesPlanRenderTest {

    private val n1 = NodeId("node-01")
    private val n2 = NodeId("node-02")
    private val n3 = NodeId("node-03")

    @Test
    fun testNothingToDo() {
        val plan = InitNodesPlan(newNodes = emptyList(), initializedNodes = listOf(n1, n2), unsafeNodes = emptyList())

        assertEquals("Every node has its id already", plan.render().renderToText())
    }

    @Test
    fun testNewNodes() {
        val plan = InitNodesPlan(
            newNodes = listOf(NewNode(n2, 1, hasMessages = false), NewNode(n3, 2, hasMessages = false)),
            initializedNodes = listOf(n1),
            unsafeNodes = emptyList()
        )

        val expected = listOf(
            "node-02: gets its id, with bucket 1",
            "node-03: gets its id, with bucket 2",
            "node-01: has its id already",
        )
        assertEquals(expected, plan.render().renderToText().lines())
    }

    @Test
    fun testUnsafeNode() {
        val plan = InitNodesPlan(
            newNodes = listOf(NewNode(n1, 0, hasMessages = false), NewNode(n2, 1, hasMessages = true)),
            initializedNodes = emptyList(),
            unsafeNodes = listOf(n2)
        )

        val text = plan.render().renderToText(width = 400)
        assertTrue(text.contains("node-02 has messages but no id yet. Initializing it together with other nodes is unsafe"), text)
        assertTrue(text.contains("init it alone first, see Butcher.md."), text)
        assertTrue(!text.contains("has its id already"), text)
    }
}
