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
     * So, in total we will send 6,000 messages and not send 4,000.
     */
    UNTIL_FIRST_FAILURE,

    /**
     * We will send 10,000 messages chunk by chunk (chunk size is ProducerOptions.batchSize). When we encounter
     * the first chunk with an invalid message (or messages), we skip this chunk and continue sending next chunks.
     * At the end, commit all successfully sent chunks.
     *
     * For example, batchSize is 1000 and invalid message is 6,500 among 10,000 messages. In this case, we will
     * first send 6000 messages, then we will meet a chunk with an invalid message, skip it and continue sending
     * another 3000 messages.
     *
     * So, in total we will send 9,000 messages (the first 6000 and the last 3000) and not send the 1,000 in between.
     */
    INSERT_AS_MANY_AS_POSSIBLE
}
