package kolbasa.cluster.schema

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
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

    @Test
    fun test_BucketValueIsInt() {
        // If this test fails, check Node.BITS_TO_HOLD_BUCKET_VALUE and BITS_TO_HOLD_ID_VALUE calculation algorithm
        assertIs<Int>(Node.MIN_BUCKET)
        assertIs<Int>(Node.MAX_BUCKET)
    }

}
