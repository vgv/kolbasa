package kolbasa.schema

import kolbasa.cluster.schema.Node

internal data class IdentifierRange(
    val start: Long,
    val end: Long
) {

    companion object {
        fun generateRange(bucket: Int): IdentifierRange {
            val start = bucket.toLong() shl Node.BITS_TO_HOLD_ID_VALUE
            val end = start or Node.VALUE_MASK
            return IdentifierRange(start, end)
        }
    }

}
