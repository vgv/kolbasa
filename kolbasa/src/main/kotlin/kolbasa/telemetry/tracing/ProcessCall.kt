package kolbasa.telemetry.tracing

import kolbasa.consumer.Message
import kolbasa.queue.Queue
import kolbasa.worker.HandleResult

/**
 * One call of the [MessageHandler][kolbasa.worker.MessageHandler] by a
 * [QueueProcessor][kolbasa.worker.QueueProcessor], as [Tracing.process] sees it.
 */
class ProcessCall<Data> internal constructor(
    /**
     * The queue the message came from.
     */
    val queue: Queue<Data>,

    /**
     * The message the handler gets.
     */
    val message: Message<Data>,

    /**
     * The real handler call. Only [proceed] calls it.
     */
    private val action: () -> HandleResult
) {

    /**
     * Whether [proceed] was called. Kolbasa checks it after [Tracing.process] returns.
     */
    internal var proceeded = false
        private set

    /**
     * Calls the handler and returns what it decided. If the handler throws, this method throws the same. Can be
     * called only once.
     */
    fun proceed(): HandleResult {
        check(!proceeded) { "ProcessCall.proceed() can be called only once" }
        proceeded = true
        return action()
    }

    /**
     * The trace context the sender stored with [message], or an empty map if there is none.
     */
    fun traceContext(): Map<String, String> {
        return message.traceContext ?: emptyMap()
    }
}
