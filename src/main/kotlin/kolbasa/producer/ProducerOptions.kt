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
    val partialInsert: PartialInsert = PartialInsert.PROHIBITED
) {

    init {
        Checks.checkProducerName(producer)
        Checks.checkProducerBatchSize(batchSize)
    }

    private companion object {
        private const val DEFAULT_BATCH_SIZE = 500
    }
}

/**
 * Different options how to deal with messages with the same unique keys
 */
enum class DeduplicationMode {
    /**
     * Just throw an exception if there is a message with the same unique key in the queue table.
     * If you send a bunch of messages, all messages (or one batch) will not be inserted (depending on [PartialInsert] option)
     */
    ERROR,

    /**
     * Messages with the same unique keys will be silently ignored. No errors will be thrown.
     */
    IGNORE_DUPLICATES
}

/**
 * Different options how to deal with sending failures
 *
 * Imagine you want to send 10,000 messages using a single producer.send() call, but there is one
 * invalid message in these 10,000 messages which can't be sent. So, there definitely will be a sending failure.
 * Which options do we have to deal with that failure?
 *
 * We have three different options, please read comments below
 */
enum class PartialInsert {
    /**
     * All 10,000 messages fail
     */
    PROHIBITED,

    /**
     * We will send 10,000 messages chunk by chunk (chunk size is ProducerOptions.batchSize). When we encounter
     * the first chunk with an invalid message (or messages), we stop sending and commit all chunks we sent before.
     *
     * For example, batchSize is 1000 and invalid message is 6,500th among 10,000 messages. In this case, we will
     * first send 6000 messages, then we will meet a chunk with an invalid message and stop.
     *
     * So, in total we will send 6,000 messages and not send 4,000:
     * ```
     *    1..1000  : success
     * 1001..2000  : success
     * 2001..3000  : success
     * 3001..4000  : success
     * 4001..5000  : success
     * 5001..6000  : success
     * 6001..7000  : FAILURE
     * 7001..8000  : FAILURE
     * 8001..9000  : FAILURE
     * 9001..10000 : FAILURE
     * ```
     */
    UNTIL_FIRST_FAILURE,

    /**
     * We will send 10,000 messages chunk by chunk (chunk size is ProducerOptions.batchSize). When we encounter
     * the first chunk with an invalid message (or messages), we skip this chunk and continue sending next chunks.
     * At the end, commit all successfully sent chunks.
     *
     * For example, batchSize is 1000 and invalid message is 6,500th among 10,000 messages. In this case, we will
     * first send 6000 messages, then we will meet a chunk with an invalid message, skip it and continue sending
     * another 3000 messages.
     *
     * So, in total we will send 9,000 messages (the first 6000 and the last 3000) and not send the 1,000 in between:
     * ```
     *    1..1000  : success
     * 1001..2000  : success
     * 2001..3000  : success
     * 3001..4000  : success
     * 4001..5000  : success
     * 5001..6000  : success
     * 6001..7000  : FAILURE
     * 7001..8000  : success
     * 8001..9000  : success
     * 9001..10000 : success
     * ```
     */
    INSERT_AS_MANY_AS_POSSIBLE
}
