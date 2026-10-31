package kolbasa.producer

import kotlin.math.min

/**
 * A list of messages to send in one `send()` call, with the options for all of them.
 *
 * This is the fullest form of a send. The other `send()` overloads, for one message or a plain list, make a request
 * with [SendOptions.DEFAULT] for you.
 *
 * The options belong to the whole request. A message can still have its own delay and number of attempts, see
 * [MessageOptions]: they win over the request's.
 *
 * ## Chunks
 *
 * A large request is not sent as one statement. Kolbasa splits it into chunks of
 * [ProducerOptions.chunkSize][kolbasa.producer.ProducerOptions.chunkSize] messages, 500 by default, and sends one
 * `INSERT` per chunk.
 *
 * All chunks are sent in one transaction: a [DatabaseProducer][kolbasa.producer.datasource.DatabaseProducer] opens
 * it, a [ConnectionAwareProducer][kolbasa.producer.connection.ConnectionAwareProducer] uses yours. Which chunks are
 * saved when one of them fails depends on
 * [ProducerOptions.partialInsert][kolbasa.producer.ProducerOptions.partialInsert]:
 * - [PartialInsert.PROHIBITED]: none, the whole request fails.
 * - [PartialInsert.UNTIL_FIRST_FAILURE], the default: the chunks before the failed one.
 * - [PartialInsert.INSERT_AS_MANY_AS_POSSIBLE]: all chunks except the failed ones.
 *
 * The returned [SendResult] has the result of every message. Both settings can be changed for one request in
 * [options].
 *
 * ## Cluster
 *
 * In a cluster, all messages of one request go to one shard, and so to one server. The shard is
 * [SendOptions.shard], or [ProducerOptions.shard][kolbasa.producer.ProducerOptions.shard], or the one that
 * [Kolbasa.shardStrategy][kolbasa.Kolbasa.shardStrategy] picks. To spread messages over the servers, send several
 * requests.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val request = SendRequest(
 *     data = listOf(SendMessage("first"), SendMessage("second")),
 *     options = SendOptions(delay = Duration.ofMinutes(1))
 * )
 *
 * val result = producer.send(queue, request)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var request = new SendRequest<>(
 *     List.of(new SendMessage<>("first"), new SendMessage<>("second")),
 *     SendOptions.builder().delay(Duration.ofMinutes(1)).build()
 * );
 *
 * var result = producer.send(queue, request);
 * ```
 *
 * Without options, `new SendRequest<>(messages)` is enough. To send a list, `producer.send(queue, messages)` is
 * shorter still. A request without options is for code that must return a [SendRequest], such as the
 * [BatchTransform][kolbasa.worker.pipe.BatchTransform] of a pipe.
 *
 * @see SendMessage
 * @see SendOptions
 * @see SendResult
 */
data class SendRequest<Data> @JvmOverloads constructor(
    /**
     * The messages to send, each with its own metadata and options.
     */
    val data: List<SendMessage<Data>>,
    /**
     * The options for the whole request. They override [ProducerOptions][kolbasa.producer.ProducerOptions] for this
     * one `send()` call.
     *
     * The default, [SendOptions.DEFAULT], overrides nothing.
     */
    val options: SendOptions = SendOptions.DEFAULT
) {

    // Effective shard, depends of many factors
    // ----------------------------------------------------------------------------------------------
    internal var effectiveShard: ShardId = ShardId.MIN_SHARD_ID
    // ----------------------------------------------------------------------------------------------

    /**
     * Create a new [SendRequest] with the same options, but with a subset of the data
     */
    private fun makeView(firstIndex: Int, lastIndex: Int): SendRequest<Data> {
        val partialCopy = this.copy(
            data = data.subList(firstIndex, lastIndex),
            options = options,
        )
        partialCopy.effectiveShard = effectiveShard
        return partialCopy
    }

    /**
     * Split this [SendRequest] into multiple [SendRequest]s with the same options, but with smaller data chunks
     */
    internal fun chunked(chunkSize: Int): Sequence<SendRequest<Data>> {
        return if (data.size <= chunkSize) {
            // Just an optimization to avoid useless allocation
            sequenceOf(this)
        } else {
            (data.indices step chunkSize).asSequence().map { from ->
                val to = min(from + chunkSize, data.size)
                makeView(from, to)
            }
        }
    }

}
