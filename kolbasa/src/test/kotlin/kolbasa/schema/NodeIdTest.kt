package kolbasa.schema

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Test

class NodeIdTest {

    @Test
    fun testEquality() {
        // It's important that (nodeId == otherNodeId) if (nodeId.id == otherNodeId.id)
        val first = NodeId("bugaga")
        val second = NodeId("bugaga")

        assertNotSame(first, second)
        assertEquals(first, second)
    }

}
