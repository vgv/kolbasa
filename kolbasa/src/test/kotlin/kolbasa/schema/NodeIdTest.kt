package kolbasa.schema

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class NodeIdTest {

    @Test
    fun testEqualsAndHashCode() {
        val first = NodeId("bugaga")
        val second = NodeId("bugaga")
        val third = NodeId("not bugaga")

        assertNotSame(first, second)
        assertEquals(first, second)
        assertEquals(first.hashCode(), second.hashCode())

        assertNotEquals(first, third)
    }

    @Test
    fun testComparison() {
        val a = NodeId("a")
        val b = NodeId("b")
        val c = NodeId("c")

        assertEquals(0, a.compareTo(a))
        assertTrue(a < b)
        assertTrue(b < c)
        assertTrue(a < c)
    }

    @Test
    fun testToString() {
        val nodeId = NodeId("testNode")
        assertEquals("node[testNode]", nodeId.toString())
    }

}
