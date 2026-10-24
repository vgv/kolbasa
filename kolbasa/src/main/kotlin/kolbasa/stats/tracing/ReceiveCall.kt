package kolbasa.stats.tracing

import kolbasa.consumer.Message
import kolbasa.queue.Queue
import kolbasa.schema.NodeId

/**
 * One `receive()` call, as [Tracing.receive] sees it.
 */
class ReceiveCall<Data> internal constructor(
    /**
     * The queue the messages come from.
     */
    val queue: Queue<Data>,

    /**
     * The node the messages come from. Its id is empty when the consumer doesn't work with a cluster.
     */
    val nodeId: NodeId,

    /**
     * The real receive. Only [proceed] calls it.
     */
    private val action: () -> List<Message<Data>>
) {

    /**
     * Whether [proceed] was called. Kolbasa checks it after [Tracing.receive] returns.
     */
    internal var proceeded = false
        private set

    /**
     * Receives the messages and returns them. Can be called only once.
     */
    fun proceed(): List<Message<Data>> {
        check(!proceeded) { "ReceiveCall.proceed() can be called only once" }
        proceeded = true
        return action()
    }

    /**
     * The trace context the sender stored with [message], or an empty map if there is none.
     */
    fun traceContext(message: Message<Data>): Map<String, String> {
        return message.traceContext ?: emptyMap()
    }
}
