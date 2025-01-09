package kolbasa.producer

import kolbasa.queue.Checks

data class SendOptions(
    /**
     * @see [ProducerOptions.deduplicationMode]
     */
    val deduplicationMode: DeduplicationMode = DeduplicationMode.ERROR,

    /**
     * @see [ProducerOptions.batchSize]
     */
    val batchSize: Int = ProducerOptions.DEFAULT_BATCH_SIZE,

    /**
     * @see [ProducerOptions.partialInsert]
     */
    val partialInsert: PartialInsert = PartialInsert.UNTIL_FIRST_FAILURE,

    val shard: Int? = null // TODO
) {

    init {
        Checks.checkBatchSize(batchSize)
    }

    class Builder internal constructor() {
        private var deduplicationMode: DeduplicationMode = DeduplicationMode.ERROR
        private var batchSize: Int = ProducerOptions.DEFAULT_BATCH_SIZE
        private var partialInsert: PartialInsert = PartialInsert.UNTIL_FIRST_FAILURE
        private var shard: Int? = null

        fun deduplicationMode(deduplicationMode: DeduplicationMode) = apply { this.deduplicationMode = deduplicationMode }
        fun batchSize(batchSize: Int) = apply { this.batchSize = batchSize }
        fun partialInsert(partialInsert: PartialInsert) = apply { this.partialInsert = partialInsert }
        fun shard(shard: Int) = apply { this.shard = shard }

        fun build() = SendOptions(deduplicationMode, batchSize, partialInsert, shard)
    }

    companion object {
        internal val SEND_OPTIONS_NOT_SET = SendOptions()

        @JvmStatic
        fun builder() = Builder()
    }

}
