package kolbasa.producer

/**
 * Full record id
 *
 * This class is a container for all the parts that make up an ID. Only all components, only full ID uniquely
 * identifies the record. Any part doesn't make sense without the others.
 */
data class Id(
    val localId: Long,
    val shard: Int
) {

    override fun toString(): String {
        return "$localId/$shard"
    }

    companion object {

        /**
         * Parses the string representation of the ID
         *
         * String format: `localId/shard`
         */
        fun fromString(stringId: String): Id {
            // Optimized implementation without string allocations and 2.5x faster than naive implementation
            var index = stringId.length - 1

            var shard = 0
            var multiplier10 = 1
            var ch = stringId[index]
            while (ch != '/') {
                shard += (ch - '0') * multiplier10
                multiplier10 *= 10
                ch = stringId[--index]
            }

            index--

            var localId = 0L
            for (i in 0..index) {
                localId = localId * 10 + (stringId[i] - '0')
            }

            return Id(localId, shard)
        }
    }

}
