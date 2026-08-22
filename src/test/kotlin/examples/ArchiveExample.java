package examples;

import kolbasa.consumer.datasource.DatabaseConsumer;
import kolbasa.producer.datasource.DatabaseProducer;
import kolbasa.queue.ArchiveQueueOptions;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.queue.QueueOptions;
import kolbasa.schema.SchemaHelpers;

import java.time.Duration;
import java.util.Objects;

class ArchiveExample {
    public static void main(String[] args) {
        // Define queue with Archive enabled.
        // When a consumer deletes a message after successful processing, the message is atomically
        // moved to the Archive queue instead of being permanently deleted. This is useful for
        // auditing, compliance, trailing, or replaying successfully processed messages.
        var queue = new Queue<>(
                "payments",
                PredefinedDataTypes.String,
                QueueOptions.builder()
                        .enableArchiveQueue(new ArchiveQueueOptions(Duration.ofDays(90), 1_000_000L))
                        .build()
        );

        // Valid datasource from DI, static factory etc.
        var dataSource = ExamplesDataSourceProvider.getDataSource();

        // Update PostgreSQL schema.
        // This creates both the main queue table (q_payments) and the Archive table (q_payments_arc)
        // automatically.
        SchemaHelpers.createOrUpdateQueues(dataSource, queue);

        // -------------------------------------------------------------------------------------------
        // Send a few messages
        var producer = new DatabaseProducer(dataSource);
        producer.send(queue, "Payment #001 — $100.00");
        producer.send(queue, "Payment #002 — $250.00");
        producer.send(queue, "Payment #003 — $75.50");

        // -------------------------------------------------------------------------------------------
        // Process messages normally. When we call consumer.delete(), each message is atomically
        // moved to the Archive queue instead of being permanently deleted.
        var consumer = new DatabaseConsumer(dataSource);
        var messages = consumer.receive(queue, 10);
        messages.forEach(message -> System.out.println("Processing: " + message.getData()));
        consumer.delete(queue, messages);
        System.out.println("Deleted " + messages.size() + " messages (moved to archive)");

        // -------------------------------------------------------------------------------------------
        // The archive queue is a regular Queue object, so we can read from it using the standard
        // Consumer API. This is useful for auditing, replaying, or exporting processed messages.
        var archive = Objects.requireNonNull(queue.getArchiveQueue());
        var archivedMessages = consumer.receive(archive, 10);
        System.out.println("\nArchived messages:");
        archivedMessages.forEach(message -> System.out.println("  " + message.getData()));
    }
}
