package kolbasa.consumer.worker

import kolbasa.consumer.datasource.Consumer
import kolbasa.producer.Id
import kolbasa.queue.Queue

/**
 * Processes one batch of messages at a time: receives it, passes every message to your [MessageHandler] and deletes
 * the messages it handled.
 *
 * Every [process] call does one batch and returns. Who calls it and when is up to you: a loop in your own thread, a
 * scheduler, or a `QueueWorker` with as many threads as you need. Works with any [Consumer], so with a
 * [ClusterConsumer][kolbasa.cluster.ClusterConsumer] too.
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
 * val processor = QueueProcessor(consumer, queue) { message ->
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
 * var processor = new QueueProcessor<>(consumer, queue, message -> {
 *     process(message.getData());
 *     return HandleResult.DONE;
 * });
 *
 * var result = processor.process();
 * ```
 */
class QueueProcessor<Data> @JvmOverloads constructor(
    /**
     * The consumer that receives and deletes the messages.
     */
    private val consumer: Consumer,

    /**
     * The queue to process.
     */
    private val queue: Queue<Data>,

    /**
     * How many messages to receive, with which options, and what to do with errors.
     */
    private val options: QueueProcessorOptions = QueueProcessorOptions.DEFAULT,

    /**
     * Your business code for one message.
     */
    private val handler: MessageHandler<Data>
) {

    /**
     * Receives one batch, passes every message to the [MessageHandler] and deletes the ones it handled, all of them
     * in one `delete()` call at the end. A skipped message stays in the queue. A message that the handler failed to
     * handle stays in the queue too, and goes to the [ProcessErrorHandler].
     *
     * If `receive()` or `delete()` fails, for example because the database is down, the exception is thrown from
     * here. What to do then, wait and retry or give up, is up to the caller.
     *
     * @return how many messages were handled, skipped and failed
     */
    fun process(): ProcessResult {
        val messages = consumer.receive(queue, options.receiveLimit, options.receiveOptions)
        if (messages.isEmpty()) {
            return EMPTY_RESULT
        }

        val succeeded = ArrayList<Id>(messages.size)
        var skipped = 0
        var failed = 0

        for (message in messages) {
            try {
                when (handler.handle(message)) {
                    HandleResult.DONE -> succeeded += message.id
                    HandleResult.SKIP -> skipped++
                }
            } catch (e: Exception) {
                options.errorHandler.onProcessError(ProcessError(queue, message, e))
                failed++
            }
        }

        if (succeeded.isNotEmpty()) {
            consumer.delete(queue, succeeded)
        }

        return ProcessResult(
            succeeded = succeeded.size,
            skipped = skipped,
            failed = failed
        )
    }

    private companion object {
        val EMPTY_RESULT = ProcessResult(succeeded = 0, skipped = 0, failed = 0)
    }
}
