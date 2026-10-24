package kolbasa.stats.tracing

import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.queue.Queue
import kolbasa.schema.NodeId

/**
 * One `send()` call, as [Tracing.send] sees it.
 */
class SendCall<Data> internal constructor(
    /**
     * The queue the messages go to.
     */
    val queue: Queue<Data>,

    /**
     * The node the messages go to. Its id is empty when the producer doesn't work with a cluster.
     */
    val nodeId: NodeId,

    /**
     * What is being sent.
     */
    val request: SendRequest<Data>,

    /**
     * The real send. Only [proceed] calls it.
     */
    private val action: (traceContext: Map<String, String>) -> SendResult<Data>
) {

    /**
     * Whether [proceed] was called. Kolbasa checks it after [Tracing.send] returns.
     */
    internal var proceeded = false
        private set

    /**
     * Sends the request and returns the result.
     *
     * [traceContext] is stored with every message of the request, an empty map stores nothing. Can be called only once.
     */
    fun proceed(traceContext: Map<String, String>): SendResult<Data> {
        check(!proceeded) { "SendCall.proceed() can be called only once" }
        proceeded = true
        return action(traceContext)
    }
}
