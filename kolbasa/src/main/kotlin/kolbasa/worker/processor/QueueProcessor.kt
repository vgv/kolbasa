package kolbasa.worker.processor

import kolbasa.Kolbasa
import kolbasa.consumer.datasource.Consumer
import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.telemetry.tracing.ProcessCall
import kolbasa.worker.BatchProcessor
import kolbasa.worker.ProcessResult
import kolbasa.worker.QueueWorker

/**
 * Processes one batch of messages at a time: receives it, passes every message to your [MessageHandler] and deletes
 * the messages it handled.
 *
 * Every [process] call does one batch and returns. Who calls it and when is up to you: a loop in your own thread, a
 * scheduler, or a [QueueWorker] with as many threads as you need. Works with any [Consumer], so with a
 * [ClusterConsumer][kolbasa.cluster.ClusterConsumer] too.
 *
 * Every handler call goes through [Tracing.process][kolbasa.telemetry.tracing.Tracing.process]. The hook is the
 * consumer's [ConsumerOptions.tracing][kolbasa.consumer.ConsumerOptions.tracing], or the global [Kolbasa.tracing] if
 * the consumer has none.
 *
 * Delivery is at least once. A message can be handled more than once: if the batch takes longer than the visibility
 * timeout (see [QueueProcessorOptions.receiveLimit]), or if the final delete fails. So the handler should be ready
 * to see the same message again.
 *
 * The processor is thread-safe and holds no state between calls, so several threads can call [process] on the same
 * instance at once.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val processor = QueueProcessor(queue, consumer) { message ->
 *     process(message.data)
 *     HandleResult.DONE
 * }
 *
 * val result = processor.process()
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var processor = new QueueProcessor<>(queue, consumer, message -> {
 *     process(message.getData());
 *     return HandleResult.DONE;
 * });
 *
 * var result = processor.process();
 * ```
 */
class QueueProcessor<Data> @JvmOverloads constructor(
    /**
     * The queue to process.
     */
    override val queue: Queue<Data>,

    /**
     * The consumer that receives and deletes the messages.
     */
    private val consumer: Consumer,

    /**
     * How many messages to receive, with which options, and what to do with errors.
     */
    private val options: QueueProcessorOptions = QueueProcessorOptions.DEFAULT,

    /**
     * Your business code for one message.
     */
    private val handler: MessageHandler<Data>
) : BatchProcessor {

    // The result of every empty batch, made once to avoid extra allocations
    private val emptyResult = ProcessResult(succeeded = 0, skipped = 0, failed = 0, requested = options.receiveLimit)

    /**
     * Receives one batch, passes every message to the [MessageHandler] and deletes the ones it handled, all of them
     * in one `delete()` call at the end. A skipped message stays in the queue. A message that the handler failed to
     * handle stays in the queue too, and goes to the [ProcessErrorHandler].
     *
     * If `receive()` or `delete()` fails, for example because the database is down, the exception is thrown from
     * here. What to do then, wait and retry or give up, is up to the caller.
     *
     * @return how many messages were handled, skipped and failed, and whether the batch was full
     */
    override fun process(): ProcessResult {
        val messages = consumer.receive(queue, options.receiveLimit, options.receiveOptions)
        if (messages.isEmpty()) {
            return emptyResult
        }

        val tracing = consumer.options.tracing ?: Kolbasa.tracing

        val succeeded = ArrayList<Id>(messages.size)
        var skipped = 0
        var failed = 0

        for (message in messages) {
            val call = ProcessCall(queue, message) { handler.handle(message) }
            try {
                when (tracing.process(call)) {
                    HandleResult.DONE -> succeeded += message.id
                    HandleResult.SKIP -> skipped++
                }
            } catch (e: Exception) {
                options.errorHandler.onProcessError(ProcessError(queue, message, e))
                failed++
            }

            // A hook that never called the handler is a bug, not a failed message. Thrown before the delete, so the
            // message stays in the queue
            check(call.proceeded) { "Tracing.process() returned without calling ProcessCall.proceed()" }
        }

        if (succeeded.isNotEmpty()) {
            consumer.delete(queue, succeeded)
        }

        return ProcessResult(
            succeeded = succeeded.size,
            skipped = skipped,
            failed = failed,
            requested = options.receiveLimit
        )
    }
}
