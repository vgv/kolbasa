package kolbasa.cluster.schema

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NodeTest {

    @Test
    fun testNodeClassIsComparable() {
        val smaller = Node("a", null)
        val bigger = Node("b", null)
        val other = Node("b", null)

        assertTrue(smaller < bigger, "Smaller: $smaller, bigger: $bigger")
        assertTrue(smaller < other, "Smaller: $smaller, bigger: $bigger")
        assertEquals(0, bigger.compareTo(other), "Bigger: $bigger, other: $other")
    }

}
