package kolbasa.example

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kolbasa.worker.pipe.QueuePipe

fun main() {
    // Imitate two PostgreSQL servers
    val sourceDataSource = ExamplesDataSourceProvider.getDataSource()
    val targetDataSource = ExamplesDataSourceProvider.getDataSource()

    // Orders come to the source queue as plain text. The target queue gets them in upper case
    val sourceQueue = Queue.of("orders", PredefinedDataTypes.String)
    val targetQueue = Queue.of("orders_copy", PredefinedDataTypes.String)

    // Update PostgreSQL schema, every queue on its own server
    SchemaHelpers.createOrUpdateQueues(sourceDataSource, sourceQueue)
    SchemaHelpers.createOrUpdateQueues(targetDataSource, targetQueue)

    // -------------------------------------------------------------------------------------------
    // Send 25 orders to the source queue
    DatabaseProducer(sourceDataSource).send(sourceQueue, (1..25).map { SendMessage("order $it") })

    // -------------------------------------------------------------------------------------------
    // The pipe takes a batch from the source queue, turns every message into one message for the target queue,
    // sends the whole batch in one call and deletes it from the source queue. Run it next to the source queue: then
    // only the send goes the long way.
    // If you don't need to change the messages, use QueuePipe.asIs: it moves them without even reading the data
    val pipe = QueuePipe.map(
        from = sourceQueue,
        consumer = DatabaseConsumer(sourceDataSource),
        to = targetQueue,
        producer = DatabaseProducer(targetDataSource)
    ) { message ->
        SendMessage(message.data.uppercase())
    }

    // Every process() call moves one batch: receive, transform, one send, one delete. Here we call it until the
    // source queue is empty. A real application gives the pipe to a QueueWorker instead, like a QueueProcessor in
    // QueueWorkerExample: QueueWorker(pipe).start()
    do {
        val result = pipe.process()
        println("Moved ${result.succeeded} orders")
    } while (result.isBatchFull)

    // -------------------------------------------------------------------------------------------
    // The orders are on the target server now
    val moved = DatabaseConsumer(targetDataSource).receive(targetQueue, limit = 100)
    println("Target queue has ${moved.size} orders, for example '${moved.first().data}'")
}
