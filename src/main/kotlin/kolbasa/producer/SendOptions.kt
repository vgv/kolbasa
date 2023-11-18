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
    val partialInsert: PartialInsert = PartialInsert.PROHIBITED
) {

    init {
        Checks.checkBatchSize(batchSize)
    }

    internal companion object {

        val SEND_OPTIONS_NOT_SET = SendOptions()

    }

}
