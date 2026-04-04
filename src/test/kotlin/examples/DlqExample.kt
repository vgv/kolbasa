package examples

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.sweep.SweepHelper
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.DlqOptions
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.schema.SchemaHelpers
import kolbasa.utils.JdbcHelpers.useConnection
import java.time.Duration

fun main() {
    // Define queue with DLQ enabled.
    // When a message exhausts all processing attempts, it will be moved to the Dead Letter Queue
    // instead of being permanently deleted. This allows failed messages to be inspected, debugged,
    // or reprocessed later.
    val queue = Queue(
        name = "orders",
        databaseDataType = PredefinedDataTypes.String,
        options = QueueOptions(
            // Only 1 attempt for this example, so the message goes to DLQ after the first failed processing
            defaultAttempts = 1,
            // Zero visibility timeout so the exhausted message becomes visible to sweep immediately.
            // This is only for example purposes — in production it should be greater than zero for 99.9999% of cases.
            defaultVisibilityTimeout = Duration.ZERO,
            dlqOptions = DlqOptions(
                retention = Duration.ofDays(14),
                maxMessages = 100_000
            )
        )
    )

    // Valid datasource from DI, static factory etc.
    val dataSource = ExamplesDataSourceProvider.getDataSource()

    // Update PostgreSQL schema.
    // This creates both the main queue table (q_orders) and the DLQ table (q_orders_dlq) automatically.
    SchemaHelpers.createOrUpdateQueues(dataSource, queue)

    // -------------------------------------------------------------------------------------------
    // Send a message to the queue
    val producer = DatabaseProducer(dataSource)
    producer.send(queue, "Order #12345")

    // -------------------------------------------------------------------------------------------
    // Receive the message but do NOT delete it (simulating a processing failure).
    // After receiving, the message's remaining_attempts is decremented. Since we configured
    // defaultAttempts = 1, the message now has 0 remaining attempts and becomes "dead".
    val consumer = DatabaseConsumer(dataSource)
    val message = consumer.receive(queue)
    println("Received from main queue: ${message?.data}")
    // Intentionally not calling consumer.delete() — the message processing "failed"

    // -------------------------------------------------------------------------------------------
    // Invoke sweep manually to move the dead message to the DLQ.
    // By default, sweep is probabilistic (every 10,000 receive/delete calls), which is efficient in production
    // but not suitable for a short example, so, for demonstration purposes, we invoke it manually here.
    // Sweep detects the dead message (remaining_attempts = 0) and moves it to the DLQ.
    dataSource.useConnection { connection ->
        SweepHelper.sweep(connection, queue, limit = 100)
    }
    
    // -------------------------------------------------------------------------------------------
    // Now we can inspect the DLQ. The dead letter queue is a regular Queue object, so it works
    // with all existing APIs — Consumer, Producer, Inspector, Mutator — without any special methods.
    val dlq = requireNotNull(queue.deadLetterQueue)
    val deadMessage = consumer.receive(dlq)
    println("Received from DLQ: ${deadMessage?.data}")

    // -------------------------------------------------------------------------------------------
    // Reprocess: send the failed message back to the main queue for another attempt
    if (deadMessage != null) {
        producer.send(queue, deadMessage.data)
        consumer.delete(dlq, deadMessage)
        println("Message requeued for reprocessing")
    }

    // Verify the message is back in the main queue
    val reprocessed = consumer.receive(queue)
    println("Reprocessed from main queue: ${reprocessed?.data}")
    if (reprocessed != null) {
        consumer.delete(queue, reprocessed)
    }
}
