package kolbasa.schema

import kolbasa.cluster.schema.Node
import kotlin.test.Test
import kotlin.test.assertEquals

class IdentifierRangeTest {

    @Test
    fun testGenerateRange() {
        (Node.MIN_BUCKET..Node.MAX_BUCKET).forEach { bucket ->
            val range = IdentifierRange.generateRange(bucket)

            // Very stupid but alternative way to generate bucket ranges
            val bucketString = bucket.toString(2).padStart(Node.BITS_TO_HOLD_BUCKET_VALUE, '0')
            val startString = bucketString + "0".repeat(Node.BITS_TO_HOLD_ID_VALUE)
            val endString = bucketString + "1".repeat(Node.BITS_TO_HOLD_ID_VALUE)

            assertEquals(startString.toLong(2), range.start)
            assertEquals(endString.toLong(2), range.end)
        }
    }

}
