package kolbasa.producer

import kolbasa.cluster.Shard
import kotlin.math.min

data class SendRequest<Data, Meta : Any>(
    /**
     * List of messages, metadata (if any) and options (if any) to send
     */
    val data: List<SendMessage<Data, Meta>>,
    /**
     * Options for sending this list of messages, allows to override [ProducerOptions][kolbasa.producer.ProducerOptions] options
     */
    val sendOptions: SendOptions = SendOptions.SEND_OPTIONS_NOT_SET
) {

    // Effective shard, depends of many factors
    // ----------------------------------------------------------------------------------------------
    internal var effectiveShard: Int = Shard.MIN_SHARD
    // ----------------------------------------------------------------------------------------------

    // OpenTelemetry
    // ----------------------------------------------------------------------------------------------
    internal var openTelemetryContext: MutableList<String>? = null

    internal fun addOpenTelemetryContext(key: String, value: String) {
        if (openTelemetryContext == null) {
            openTelemetryContext = mutableListOf()
        }

        openTelemetryContext!!.add(key)
        openTelemetryContext!!.add(value)
    }
    // ----------------------------------------------------------------------------------------------

    /**
     * Create a new [SendRequest] with the same options, but with a subset of the data
     */
    private fun makeView(firstIndex: Int, lastIndex: Int): SendRequest<Data, Meta> {
        val partialCopy = this.copy(
            data = data.subList(firstIndex, lastIndex),
            sendOptions = sendOptions,
        )
        partialCopy.effectiveShard = effectiveShard
        partialCopy.openTelemetryContext = openTelemetryContext
        return partialCopy
    }

    /**
     * Split this [SendRequest] into multiple [SendRequest]s with the same options, but with smaller data chunks
     */
    internal fun chunked(chunkSize: Int): Sequence<SendRequest<Data, Meta>> {
        return if (data.size <= chunkSize) {
            // Just an optimization
            sequenceOf(this)
        } else {
            (data.indices step chunkSize).asSequence().map {
                val from = it
                val to = min(it + chunkSize, data.size)
                makeView(from, to)
            }
        }
    }

}
