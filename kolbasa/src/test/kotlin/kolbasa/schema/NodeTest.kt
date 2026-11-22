package kolbasa.schema

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class NodeTest {

    @Test
    fun testNodeClassIsComparable() {
        val smaller = Node(NodeId("a"), 1)
        val bigger = Node(NodeId("b"), 2)
        val other = Node(NodeId("b"), 3)

        assertTrue(smaller < bigger, "Smaller: $smaller, bigger: $bigger")
        assertTrue(smaller < other, "Smaller: $smaller, bigger: $bigger")
        assertEquals(0, bigger.compareTo(other), "Bigger: $bigger, other: $other")
    }

    @Test
    fun testNodeIsItsId() {
        // The same id, another bucket: the same node, the same as in compareTo
        val node = Node(NodeId("a"), 1)
        val sameNode = Node(NodeId("a"), 2)

        assertEquals(node, sameNode)
        assertEquals(node.hashCode(), sameNode.hashCode())
        assertNotEquals(node, Node(NodeId("b"), 1))
    }

    @Test
    fun testToString() {
        assertEquals("node[node-01,5]", Node(NodeId("node-01"), 5).toString())
    }

}
