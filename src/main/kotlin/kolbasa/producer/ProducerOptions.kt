package kolbasa.producer

import kolbasa.queue.Checks
import java.util.concurrent.ExecutorService

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
    val partialInsert: PartialInsert = PartialInsert.UNTIL_FIRST_FAILURE,

    /**
     * Shard number.
     *
     * If you are using Kolbasa Cluster and have multiple PostgreSQL servers, messages will be distributed among these servers
     * in a non-deterministic, unknown manner. However, in some cases, it may be useful to control how messages are
     * distributed across the servers. The two most common scenarios are:
     * 1) Deduplication using unique meta fields. Kolbasa uses unique PostgreSQL indexes for deduplication, but unique indexes
     *    do not work across multiple database servers. Therefore, messages must be on the same server for this to work correctly.
     * 2) Enforcing message order when reading. For example, you want to put messages with specific `account_id` and read them
     *    later in the same order (by another field or just time of sending). You can use filtering by `account_id` and sorting
     *    by required field (fields), but, if messages are not on the same server, you can't have an order guarantee. To ensure
     *    correct ordering, you must ensure that messages with the same `account_id` are on the same server.
     *
     * To control message persistence on specific servers in the cluster shard field should be used.
     * Kolbasa guarantees that messages with the same shard value will be stored on the same server. The shard value can be any
     * integer from [Int.MIN_VALUE] to [Int.MAX_VALUE]. For Kolbasa itself, the specific shard value carries no inherent
     * meaning — it is arbitrary and determined by your business logic. The only guarantee is that messages with the same shard
     * will persist on the same server. Messages with different shard values may be stored on the same server or on different
     * ones, without any guarantee.
     */
    val shard: Int? = null,

    /**
     * Executor used to send messages asynchronously in [Producer][kolbasa.producer.datasource.Producer] sendAsync() methods.
     *
     * If you need to customize the executor for a specific [Producer][kolbasa.producer.datasource.Producer], you can
     * provide your own [ExecutorService]. If you don't provide a custom executor, producer will use the global,
     * default executor defined in [Kolbasa.asyncExecutor][kolbasa.Kolbasa.asyncExecutor]
     */
    val asyncExecutor: ExecutorService? = null,
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
        private var shard: Int? = null
        private var asyncExecutor: ExecutorService? = null

        fun producer(producer: String) = apply { this.producer = producer }
        fun deduplicationMode(deduplicationMode: DeduplicationMode) = apply { this.deduplicationMode = deduplicationMode }
        fun batchSize(batchSize: Int) = apply { this.batchSize = batchSize }
        fun partialInsert(partialInsert: PartialInsert) = apply { this.partialInsert = partialInsert }
        fun shard(shard: Int) = apply { this.shard = shard }
        fun asyncExecutor(asyncExecutor: ExecutorService) = apply { this.asyncExecutor = asyncExecutor }

        fun build() = ProducerOptions(producer, deduplicationMode, batchSize, partialInsert, shard, asyncExecutor)
    }

    companion object {
        internal const val DEFAULT_BATCH_SIZE = 500

        @JvmStatic
        fun builder() = Builder()
    }
}

