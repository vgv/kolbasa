package kolbasa.schema

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame

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
