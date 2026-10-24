package kolbasa.stats.tracing

import kolbasa.consumer.Message
import kolbasa.producer.SendResult

/**
 * A hook for a tracing system.
 *
 * Kolbasa calls it around every `send()` and every `receive()`. The hook does its own work before and after
 * `proceed()`, and `proceed()` does the real send or receive. When sending, the hook can pass a trace context, and
 * Kolbasa stores it with the messages. When receiving, the hook can read that context back from every message.
 *
 * The hook can't change what is sent or received. If you need that, for example to log calls or to add meta fields,
 * wrap [Producer][kolbasa.producer.datasource.Producer] or [Consumer][kolbasa.consumer.datasource.Consumer] instead.
 *
 * An implementation must call `proceed()` exactly once, before it returns, and return what `proceed()` returned.
 * Kolbasa checks the first part and throws [IllegalStateException] if `proceed()` was called twice or not at all.
 *
 * Set it for the whole application with [Kolbasa.tracing][kolbasa.Kolbasa.tracing], or for one producer or consumer
 * with [ProducerOptions.tracing][kolbasa.producer.ProducerOptions.tracing] and
 * [ConsumerOptions.tracing][kolbasa.consumer.ConsumerOptions.tracing].
 */
interface Tracing {

    /**
     * Called around every `send()`. Must call [SendCall.proceed] exactly once and return its result.
     */
    fun <Data> send(call: SendCall<Data>): SendResult<Data>

    /**
     * Called around every `receive()`. Must call [ReceiveCall.proceed] exactly once and return its result.
     */
    fun <Data> receive(call: ReceiveCall<Data>): List<Message<Data>>

    companion object {

        /**
         * No tracing: every call goes straight through, and no trace context is stored or read. The default.
         */
        @JvmField
        val NONE: Tracing = object : Tracing {
            override fun <Data> send(call: SendCall<Data>) = call.proceed(emptyMap())
            override fun <Data> receive(call: ReceiveCall<Data>) = call.proceed()
        }
    }
}
