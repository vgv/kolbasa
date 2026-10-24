package kolbasa.stats.tracing

import kolbasa.consumer.Message
import kolbasa.producer.SendResult
import java.util.function.Supplier

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
 *
 * For OpenTelemetry, use `OpenTelemetryTracing` from the separate `io.github.vgv:kolbasa-opentelemetry` artifact.
 *
 * ## Usage Example
 *
 * ```kotlin
 * // for the whole application
 * Kolbasa.tracing = OpenTelemetryTracing(openTelemetry)
 * // or for one consumer only
 * val consumer = DatabaseConsumer(dataSource, ConsumerOptions(tracing = OpenTelemetryTracing(openTelemetry)))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setTracing(new OpenTelemetryTracing(openTelemetry));
 * ```
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

    /**
     * Called by every `sendAsync()`, in the caller's thread, before the send goes to the executor. The executor
     * runs the returned task instead of [task]. Use it to carry the caller's trace context into the executor's
     * thread, so the send span gets the right parent.
     *
     * By default returns [task] as is.
     */
    fun <T> wrapAsync(task: Supplier<T>): Supplier<T> = task

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
