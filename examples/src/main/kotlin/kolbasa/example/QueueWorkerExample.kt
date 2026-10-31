package kolbasa.example

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kolbasa.worker.HandleResult
import kolbasa.worker.QueueProcessor
import kolbasa.worker.QueueWorker
import kolbasa.worker.QueueWorkerOptions
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

fun main() {
    // Define queue with name `test_queue` and varchar type as data storage in PostgreSQL table
    val queue = Queue.of("test_queue", PredefinedDataTypes.String)

    // Valid datasource from DI, static factory etc.
    val dataSource = ExamplesDataSourceProvider.getDataSource()

    // Update PostgreSQL schema
    SchemaHelpers.createOrUpdateQueues(dataSource, queue)

    // -------------------------------------------------------------------------------------------
    // Send 1000 messages
    val producer = DatabaseProducer(dataSource)
    producer.send(queue, (1..1000).map { SendMessage("Message $it") })

    // -------------------------------------------------------------------------------------------
    // Your business code for one message. Return DONE, and the message is deleted from the queue.
    // Throw, and it comes back after the visibility timeout for another attempt
    val handled = CountDownLatch(1000)
    val perThread = ConcurrentHashMap<String, AtomicInteger>()
    val processor = QueueProcessor(queue, DatabaseConsumer(dataSource)) { message ->
        // ... your business code for message.data ...
        perThread.computeIfAbsent(Thread.currentThread().name) { AtomicInteger() }.incrementAndGet()
        handled.countDown()
        HandleResult.DONE
    }

    // -------------------------------------------------------------------------------------------
    // The worker calls the processor in 3 background threads, again and again. While the batches come full, a
    // thread takes the next one at once. When the queue is empty, it waits a bit and asks again
    val worker = QueueWorker(processor, QueueWorkerOptions(threads = 3))
    worker.start()

    // A real application lets the worker run until the application stops. Here we wait for the 1000 messages only
    handled.await()

    // Stop the worker: the threads finish their batches and end. Its threads are not daemons, so without this the
    // program would never exit
    worker.stop(Duration.ofSeconds(10))

    // Both threads took their share of the batches
    perThread.toSortedMap().forEach { (thread, count) -> println("$thread handled $count messages") }
}
