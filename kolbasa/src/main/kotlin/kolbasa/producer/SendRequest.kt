package kolbasa.producer

import kotlin.math.min

data class SendRequest<Data>(
    /**
     * List of messages, metadata (if any) and options (if any) to send
     */
    val data: List<SendMessage<Data>>,
    /**
     * Options for sending this list of messages, allows to override [ProducerOptions][kolbasa.producer.ProducerOptions] options
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
