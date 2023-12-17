package kolbasa.producer

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
