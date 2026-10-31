package kolbasa.worker.pipe

import kolbasa.consumer.datasource.Consumer
import kolbasa.producer.Id
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.Producer
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.worker.BatchProcessor
import kolbasa.worker.ProcessError
import kolbasa.worker.ProcessErrorHandler
import kolbasa.worker.ProcessResult
import kolbasa.worker.QueueWorker

/**
 * Moves messages from one queue to another, usually on another server. One batch at a time: receives it from the
 * source queue, transforms it, sends all of it to the target queue in one `send()` call, and deletes the source
 * messages in one `delete()` call.
 *
 * Make a pipe with one of the factories:
 * - [asIs]: moves the messages as they are, without reading them into `Data`;
 * - [map]: turns every message into one message for the target queue with your [MapTransform];
 * - [flatMap]: turns every message into any number of messages, or none to filter it out, with your
 *   [FlatMapTransform];
 * - [mapBatch]: turns a whole batch into one [SendRequest][kolbasa.producer.SendRequest] with your [BatchTransform],
 *   so you choose the shard and the send options of every batch.
 *
 * Every [process] call does one batch and returns. Who calls it and when is up to you: a loop in your own thread, a
 * scheduler, or a [QueueWorker]. Works with any [Consumer] and [Producer], so with a
 * [ClusterConsumer][kolbasa.cluster.ClusterConsumer] and a [ClusterProducer][kolbasa.cluster.ClusterProducer] too.
 *
 * If the queues are far apart, for example on different continents, run the pipe next to the source queue. Then only
 * one call per batch, the send, goes the long way.
 *
 * **A batch is sent all or nothing.** The pipe always sends with
 * [PartialInsert.PROHIBITED][kolbasa.producer.PartialInsert.PROHIBITED], whatever
 * [ProducerOptions.partialInsert][kolbasa.producer.ProducerOptions.partialInsert] or the request of a
 * [BatchTransform] says. If the send fails, nothing is deleted from the source queue. The messages come back after
 * the visibility timeout, each with one attempt spent, and the error is thrown from [process]. A [QueueWorker] then
 * waits and tries again. So one message that the target queue rejects, for example a duplicate, fails its whole
 * batch, every time. Use
 * [DeduplicationMode.IGNORE_DUPLICATE][kolbasa.producer.DeduplicationMode.IGNORE_DUPLICATE] for duplicates, and check
 * the rest in the transform.
 *
 * **Give the source queue enough attempts.** Every receive spends one attempt of a message, also when the send fails
 * after it. While the target queue is down, the attempts run out, and the messages leave the source queue for good,
 * to its DLQ if it has one. So set [QueueOptions.defaultAttempts][kolbasa.queue.QueueOptions.defaultAttempts] of the
 * source queue high enough to outlive an outage of the target.
 *
 * Delivery is at least once. A message can be sent more than once: if the delete fails after the send, or if the
 * batch takes longer than the visibility timeout. So the consumer of the target queue should be ready to see the same
 * message again.
 *
 * The pipe is thread-safe and holds no state between calls, so several threads can call [process] on the same
 * instance at once.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val pipe = QueuePipe.map(from, consumer, to, producer) { message ->
 *     SendMessage(convert(message.data))
 * }
 *
 * val result = pipe.process()
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var pipe = QueuePipe.map(from, consumer, to, producer, message -> {
 *     return new SendMessage<>(convert(message.getData()));
 * });
 *
 * var result = pipe.process();
 * ```
 */
class QueuePipe internal constructor(
    /**
     * The queue the pipe takes the messages from.
     */
    override val queue: Queue<*>,

    /**
     * Does the work, with the real types of the queues.
     */
    private val engine: PipeEngine<*, *>
) : BatchProcessor {

    /**
     * Receives one batch, transforms it, sends all of it in one `send()` call, and deletes the source messages in one
     * `delete()` call. A message that the transform failed on stays in the source queue, and goes to the
     * [ProcessErrorHandler].
     *
     * If `receive()`, `send()` or `delete()` fails, the exception is thrown from here, and no message is deleted
     * from the source queue. What to do then, wait and retry or give up, is up to the caller.
     *
     * @return how many source messages were moved and failed, and whether the batch was full. Nothing is ever
     * skipped, so [ProcessResult.skipped] is always 0
     */
    override fun process(): ProcessResult = engine.process()

    companion object {

        /**
         * Makes a pipe that moves messages as they are. The stored data goes from [from] to [to] without your
         * serializer and deserializer: it is not read into `Data` and not written back. It is faster, and nothing is
         * lost: for example, JSON fields that your class doesn't know survive. A message that your deserializer can't
         * read moves too.
         *
         * Both queues must store the data the same way: the same kind of [DatabaseQueueDataType], and the same format
         * inside. The kind is checked here. The format can't be checked, so if [to] expects another format, use [map]
         * with a transform that converts it.
         *
         * The metadata moves too, if [ReceiveOptions.readMetadata][kolbasa.consumer.ReceiveOptions.readMetadata] is
         * on in [QueuePipeOptions.receiveOptions]. It is on by default. Only the fields that [to] declares the same
         * way are written.
         *
         * @throws IllegalArgumentException if the queues store the data in different kinds of columns
         */
        @JvmStatic
        @JvmOverloads
        fun <Data> asIs(
            from: Queue<Data>,
            consumer: Consumer,
            to: Queue<Data>,
            producer: Producer,
            options: QueuePipeOptions = QueuePipeOptions.AS_IS_DEFAULT
        ): QueuePipe {
            val fromType = from.databaseDataType
            val toType = to.databaseDataType
            require(fromType.dbColumnType == toType.dbColumnType) {
                "Queues '${from.name}' and '${to.name}' store data differently: " +
                    "${fromType.dbColumnType} and ${toType.dbColumnType}"
            }

            // The same tables, but the data is read and written as it is stored, without the queue's serializer
            fun <Raw> rawEngine(rawType: DatabaseQueueDataType<Raw>): PipeEngine<Raw, Raw> {
                val rawFrom = from.withDataType(rawType)
                val rawTo = to.withDataType(rawType)
                return flatMapEngine(rawFrom, consumer, rawTo, producer, options) {
                    listOf(SendMessage(it.data, it.meta))
                }
            }

            val engine: PipeEngine<*, *> = when (fromType) {
                is DatabaseQueueDataType.Json -> rawEngine(RAW_JSON)
                is DatabaseQueueDataType.Binary -> rawEngine(PredefinedDataTypes.ByteArray)
                is DatabaseQueueDataType.Text -> rawEngine(PredefinedDataTypes.String)
                is DatabaseQueueDataType.Int -> rawEngine(PredefinedDataTypes.Int)
                is DatabaseQueueDataType.Long -> rawEngine(PredefinedDataTypes.Long)
            }

            return QueuePipe(from, engine)
        }

        /**
         * Makes a pipe that turns every message of [from] into one message for [to] with your [MapTransform].
         *
         * A message that the transform fails on stays in [from], and goes to the [ProcessErrorHandler]. The other
         * messages of the batch are sent.
         */
        @JvmStatic
        @JvmOverloads
        fun <In, Out> map(
            from: Queue<In>,
            consumer: Consumer,
            to: Queue<Out>,
            producer: Producer,
            options: QueuePipeOptions = QueuePipeOptions.DEFAULT,
            transform: MapTransform<In, Out>
        ): QueuePipe {
            val engine = flatMapEngine(from, consumer, to, producer, options) { listOf(transform.transform(it)) }
            return QueuePipe(from, engine)
        }

        /**
         * Makes a pipe that turns every message of [from] into any number of messages for [to] with your
         * [FlatMapTransform].
         *
         * An empty list filters the message out: nothing is sent for it, and it is deleted once the batch is sent. A
         * message that the transform fails on stays in [from], and goes to the [ProcessErrorHandler]. The other
         * messages of the batch are sent.
         */
        @JvmStatic
        @JvmOverloads
        fun <In, Out> flatMap(
            from: Queue<In>,
            consumer: Consumer,
            to: Queue<Out>,
            producer: Producer,
            options: QueuePipeOptions = QueuePipeOptions.DEFAULT,
            transform: FlatMapTransform<In, Out>
        ): QueuePipe {
            return QueuePipe(from, flatMapEngine(from, consumer, to, producer, options, transform))
        }

        /**
         * Makes a pipe that turns a whole batch of [from] into one [SendRequest][kolbasa.producer.SendRequest] for
         * [to] with your [BatchTransform]. The request carries the messages and the send options of the batch, the
         * shard for example.
         *
         * The batch is one unit: all its messages are deleted once the request is sent, and if the transform throws,
         * all of them stay in [from] and go to the [ProcessErrorHandler]. See [BatchTransform].
         */
        @JvmStatic
        @JvmOverloads
        fun <In, Out> mapBatch(
            from: Queue<In>,
            consumer: Consumer,
            to: Queue<Out>,
            producer: Producer,
            options: QueuePipeOptions = QueuePipeOptions.DEFAULT,
            transform: BatchTransform<In, Out>
        ): QueuePipe {
            val engine = PipeEngine(from, consumer, to, producer, options) { messages ->
                try {
                    val request = transform.transform(messages)
                    TransformedBatch(request.data, messages.map { it.id }, failed = 0, sendOptions = request.options)
                } catch (e: Exception) {
                    // The whole batch failed: every message goes to the error handler with the same cause, and stays
                    messages.forEach { options.errorHandler.onProcessError(ProcessError(from, it, e)) }
                    TransformedBatch(emptyList(), emptyList(), failed = messages.size)
                }
            }

            return QueuePipe(from, engine)
        }

        // Every message turns into any number of messages to send, map and asIs too. A message that the transform
        // failed on stays in the queue
        private fun <In, Out> flatMapEngine(
            from: Queue<In>,
            consumer: Consumer,
            to: Queue<Out>,
            producer: Producer,
            options: QueuePipeOptions,
            transform: FlatMapTransform<In, Out>
        ): PipeEngine<In, Out> {
            return PipeEngine(from, consumer, to, producer, options) { messages ->
                val toSend = ArrayList<SendMessage<Out>>(messages.size)
                val toDelete = ArrayList<Id>(messages.size)
                var failed = 0

                for (message in messages) {
                    try {
                        toSend += transform.transform(message)
                        toDelete += message.id
                    } catch (e: Exception) {
                        options.errorHandler.onProcessError(ProcessError(from, message, e))
                        failed++
                    }
                }

                TransformedBatch(toSend, toDelete, failed)
            }
        }

        private fun <Raw> Queue<*>.withDataType(rawType: DatabaseQueueDataType<Raw>): Queue<Raw> {
            return Queue(name, rawType, options, metadata, role)
        }

        // There is no predefined raw JSON type: a string that goes to a jsonb column and back
        private val RAW_JSON = DatabaseQueueDataType.Json<String>({ it }, { it })
    }
}
