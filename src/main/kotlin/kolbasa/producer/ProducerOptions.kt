package kolbasa.producer

import kolbasa.queue.Checks

data class ProducerOptions(
    /**
     * Arbitrary producer name. Every message, sent by this producer will have this name in the 'producer' column of
     * corresponding queue table.
     *
     * Used for debugging purposes only. There is no way to get this value during consuming.
     * If you feel it can be helpful to understand which producer sent a message when you debug your application by
     * exploring queue table directly in PostgreSQL, you can set this value.
     */
    val producer: String? = null,

    /**
     * Deduplication mode.
     *
     * When you try to insert a message with a unique key which already exists in the queue table, you have two options:
     * 1) Throw an exception and let the developer handle this problem. This is the default behavior.
     * 2) Silently ignore "duplicated" message. In this case, the message will not be inserted into the queue
     * table and no errors will be thrown. For example, if you try to send 10,000 messages and 100 of them have duplicated
     * unique keys, only 9,900 messages will be inserted into the queue table.
     */
    val deduplicationMode: DeduplicationMode = DeduplicationMode.ERROR,

    /**
     * Batch size for sending messages. Default value is 500.
     *
     * Batch size controls two things:
     * 1) Performance.
     * When you send N messages using one producer.send() call, Kolbasa doesn't send N separate INSERT statements
     * to PostgreSQL. Instead, it splits the list into chunks of size [batchSize] and sends one INSERT statement
     * per chunk. So, if you send 10,000 messages using one producer.send() call and [batchSize] is 500, kolbasa
     * will only make 20 calls to the database.
     *
     * 2) Failure granularity.
     * Batch size helps you to control how big (or small) has to be a 'failure' chunk. See [PartialInsert] for details.
     */
    val batchSize: Int = DEFAULT_BATCH_SIZE,
    /**
     * Partial insert strategy. See [PartialInsert] for details.
     */
    val partialInsert: PartialInsert = PartialInsert.UNTIL_FIRST_FAILURE
) {

    init {
        Checks.checkProducerName(producer)
        Checks.checkBatchSize(batchSize)
    }

    class Builder internal constructor() {
        private var producer: String? = null
        private var deduplicationMode: DeduplicationMode = DeduplicationMode.ERROR
        private var batchSize: Int = DEFAULT_BATCH_SIZE
        private var partialInsert: PartialInsert = PartialInsert.UNTIL_FIRST_FAILURE

        fun producer(producer: String?) = apply { this.producer = producer }
        fun deduplicationMode(deduplicationMode: DeduplicationMode) = apply { this.deduplicationMode = deduplicationMode }
        fun batchSize(batchSize: Int) = apply { this.batchSize = batchSize }
        fun partialInsert(partialInsert: PartialInsert) = apply { this.partialInsert = partialInsert }

        fun build() = ProducerOptions(producer, deduplicationMode, batchSize, partialInsert)
    }

    companion object {
        internal const val DEFAULT_BATCH_SIZE = 500

        @JvmStatic
        fun builder() = Builder()
    }
}

