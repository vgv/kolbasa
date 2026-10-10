package kolbasa.schema

internal data class IdRange(
    val min: Long,
    val max: Long,
    val cache: Long = Const.ID_COLUMN_IDENTITY_CACHE_VALUE
) {

    companion object {

        fun generateRange(bucket: Int): IdRange {
            val longBucket = bucket.toLong()

            val start = longBucket shl Node.BITS_TO_HOLD_ID_VALUE // the smallest possible number for a given bucket
            val end = start or Node.VALUE_MASK // the biggest possible number for a given bucket
            return IdRange(start, end)
        }

        const val MIN_ID = 0.toLong()
        const val MAX_ID = Long.MAX_VALUE

        // Min range for bucket 0, just to have it pre-generated for tests
        val MIN_RANGE = generateRange(Node.MIN_BUCKET)
    }

}
