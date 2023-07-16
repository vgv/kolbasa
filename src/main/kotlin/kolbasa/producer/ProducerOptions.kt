package kolbasa.producer

import kolbasa.queue.Checks

data class ProducerOptions(
    val producer: String? = null,
    val batchSize: Int = DEFAULT_BATCH_SIZE,
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
     * the first chunk with an invalid message (or messages), we stop sending and commit all chunks we sent before.
     *
     * For example, batchSize is 1000 and invalid message is 6,500 among 10,000 messages. In this case, we will
     * first send 6000 messages, then we will meet a chunk with an invalid message, skip it and continue sending
     * another 4000 messages.
     *
     * So, in total we will send 9,000 messages and not send 1,000.
     */
    INSERT_AS_MANY_AS_POSSIBLE
}
