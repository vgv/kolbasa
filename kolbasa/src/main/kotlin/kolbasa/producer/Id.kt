package kolbasa.producer

/**
 * Full record id
 *
 * This class is a container for all the parts that make up an ID. Only all components, only full ID uniquely
 * identifies the record. Any part doesn't make sense without the others.
 */
data class Id(
    /** The value of the queue table's `id` column. */
    val localId: Long,

    /**
     * The value of the queue table's `shard` column.
     *
     * In a cluster the shard decides which server stores the message.
     */
    val shard: ShardId
) {

    override fun toString(): String {
        return "$localId/${shard.id}"
    }

    companion object {

        /**
         * Parses the string representation of the ID
         *
         * Optimized implementation without string allocations and 2.5x faster than naive
         * implementation. String format: `localId/shard`
         *
         * **This method validates nothing and trusts its input completely.**
         *
         * 1. It always assumes the `number/number` format. A string without `/`, or an empty one, fails with
         *    [StringIndexOutOfBoundsException]; a non-digit character or a number too large for its type is not
         *    detected at all and silently produces a wrong [Id].
         * 2. It does not check that the shard is a real shard. A value outside `0..1023` is folded into that
         *    range by [ShardId.of], so the returned [Id] then points at a different shard than the string named.
         *
         * In other words, feed it only strings produced by [toString]; anything else is the caller's problem.
         */
        @JvmStatic
        fun fromString(stringId: String): Id {
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

            return Id(localId, ShardId.of(shard))
        }
    }

}
