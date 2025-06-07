package kolbasa.schema

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

}
