package examples

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.ArchiveQueueOptions
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.schema.SchemaHelpers
import java.time.Duration

fun main() {
    // Define queue with Archive enabled.
    // When a consumer deletes a message after successful processing, the message is atomically
    // moved to the Archive queue instead of being permanently deleted. This is useful for
    // auditing, compliance, trailing, or replaying successfully processed messages.
    val queue = Queue(
        name = "payments",
        databaseDataType = PredefinedDataTypes.String,
        options = QueueOptions(
            archiveQueueOptions = ArchiveQueueOptions(
                retention = Duration.ofDays(90),
                maxMessages = 1_000_000
            )
        )
    )

    // Valid datasource from DI, static factory etc.
    val dataSource = ExamplesDataSourceProvider.getDataSource()

    // Update PostgreSQL schema.
    // This creates both the main queue table (q_payments) and the Archive table (q_payments_arc)
    // automatically.
    SchemaHelpers.createOrUpdateQueues(dataSource, queue)

    // -------------------------------------------------------------------------------------------
    // Send a few messages
    val producer = DatabaseProducer(dataSource)
    producer.send(queue, "Payment #001 — \$100.00")
    producer.send(queue, "Payment #002 — \$250.00")
    producer.send(queue, "Payment #003 — \$75.50")

    // -------------------------------------------------------------------------------------------
    // Process messages normally. When we call consumer.delete(), each message is atomically
    // moved to the Archive queue instead of being permanently deleted.
    val consumer = DatabaseConsumer(dataSource)
    val messages = consumer.receive(queue, limit = 10)
    messages.forEach { message ->
        println("Processing: ${message.data}")
    }
    consumer.delete(queue, messages)
    println("Deleted ${messages.size} messages (moved to archive)")

    // -------------------------------------------------------------------------------------------
    // The archive queue is a regular Queue object, so we can read from it using the standard
    // Consumer API. This is useful for auditing, replaying, or exporting processed messages.
    val archive = requireNotNull(queue.archiveQueue)
    val archivedMessages = consumer.receive(archive, limit = 10)
    println("\nArchived messages:")
    archivedMessages.forEach { message ->
        println("  ${message.data}")
    }
}
