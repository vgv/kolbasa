package kolbasa.schema

import kolbasa.cluster.schema.Node
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class IdRangeTest {

    @Test
    fun testGenerateRange() {
        val allPossibleBuckets = (Node.MIN_BUCKET..Node.MAX_BUCKET) + Node.BUCKET_FOR_NOT_CLUSTERED_ENVIRONMENT

        allPossibleBuckets.forEach { bucket ->
            val range = IdRange.generateRange(bucket)

            // Very stupid but alternative way to generate bucket ranges
            val bucketString = bucket.toString(2).padStart(Node.BITS_TO_HOLD_BUCKET_VALUE, '0')
            val startString = bucketString + "0".repeat(Node.BITS_TO_HOLD_ID_VALUE)
            val endString = bucketString + "1".repeat(Node.BITS_TO_HOLD_ID_VALUE)

            assertEquals(startString.toLong(2), range.start)
            assertEquals(endString.toLong(2), range.end)
        }
    }

    @Test
    fun testGenerateRange_No_Intersections() {
        val allPossibleBuckets = (Node.MIN_BUCKET..Node.MAX_BUCKET) + Node.BUCKET_FOR_NOT_CLUSTERED_ENVIRONMENT

        val allPossibleRanges = allPossibleBuckets.map(IdRange.Companion::generateRange)
        for (i in 0 .. allPossibleRanges.size - 2) {
            val range = allPossibleRanges[i]

            for (j in i + 1 until allPossibleRanges.size) {
                val otherRange = allPossibleRanges[j]

                assertFalse(range intersect otherRange, "$range vs $otherRange")
            }
        }
    }

    @Test
    fun testIntersect() {
        // Just small test to test the test intersect method :)
        assertTrue(IdRange(1, 10) intersect IdRange(5, 7))
        assertTrue(IdRange(1, 10) intersect IdRange(0, 20))
        assertTrue(IdRange(1, 10) intersect IdRange(0, 5))
        assertTrue(IdRange(1, 10) intersect IdRange(7, 12))

        assertFalse(IdRange(1, 10) intersect IdRange(11, 20))
    }

    private infix fun IdRange.intersect(other: IdRange): Boolean {
        if (start <= other.start && other.end <= end) return true
        if (other.start <= start && end <= other.end) return true
        if (other.start in start..end) return true
        if (other.end in start..end) return true

        return false
    }

}
