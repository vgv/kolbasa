package kolbasa.telemetry.tracing

import kolbasa.consumer.Message
import kolbasa.consumer.worker.HandleResult
import kolbasa.producer.SendResult
import java.util.function.Supplier

/**
 * A hook for a tracing system.
 *
 * Kolbasa calls it around every `send()` and every `receive()`, and a
 * [QueueProcessor][kolbasa.consumer.worker.QueueProcessor] calls it around every message it passes to the handler.
 * The hook does its own work before and after `proceed()`, and `proceed()` does the real send, receive or handler
 * call. When sending, the hook can pass a trace context, and Kolbasa stores it with the messages. When receiving and
 * processing, the hook can read that context back from every message.
 *
 * The hook can't change what is sent or received. If you need that, for example to log calls or to add meta fields,
 * wrap [Producer][kolbasa.producer.datasource.Producer] or [Consumer][kolbasa.consumer.datasource.Consumer] instead.
 *
 * An implementation must call `proceed()` exactly once, before it returns, and return what `proceed()` returned.
 * Kolbasa checks the first part and throws [IllegalStateException] if `proceed()` was called twice or not at all.
 *
 * Every method just proceeds by default, so an implementation overrides only what it needs.
 *
 * Set it for the whole application with [Kolbasa.tracing][kolbasa.Kolbasa.tracing], or for one producer, consumer or
 * processor with [ProducerOptions.tracing][kolbasa.producer.ProducerOptions.tracing],
 * [ConsumerOptions.tracing][kolbasa.consumer.ConsumerOptions.tracing] and
 * [QueueProcessorOptions.tracing][kolbasa.consumer.worker.QueueProcessorOptions.tracing].
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
     *
     * By default just calls [SendCall.proceed] with no trace context.
     */
    fun <Data> send(call: SendCall<Data>): SendResult<Data> = call.proceed(emptyMap())

    /**
     * Called around every `receive()`. Must call [ReceiveCall.proceed] exactly once and return its result.
     *
     * By default just calls [ReceiveCall.proceed].
     */
    fun <Data> receive(call: ReceiveCall<Data>): List<Message<Data>> = call.proceed()

    /**
     * Called by a [QueueProcessor][kolbasa.consumer.worker.QueueProcessor] around every call of its
     * [MessageHandler][kolbasa.consumer.worker.MessageHandler]. Must call [ProcessCall.proceed] exactly once and
     * return its result. If `proceed()` throws, let the exception go on: the processor counts the message as failed.
     *
     * By default just calls [ProcessCall.proceed].
     */
    fun <Data> process(call: ProcessCall<Data>): HandleResult = call.proceed()

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
        val NONE: Tracing = object : Tracing {}
    }
}
