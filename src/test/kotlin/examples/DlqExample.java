package examples;

import kolbasa.consumer.datasource.DatabaseConsumer;
import kolbasa.consumer.sweep.SweepHelper;
import kolbasa.producer.datasource.DatabaseProducer;
import kolbasa.queue.DlqOptions;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.queue.QueueOptions;
import kolbasa.schema.SchemaHelpers;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

class DlqExample {
    public static void main(String[] args) throws SQLException {
        // Define queue with DLQ enabled.
        // When a message exhausts all processing attempts, it will be moved to the Dead Letter Queue
        // instead of being permanently deleted. This allows failed messages to be inspected, debugged,
        // or reprocessed later.
        var queue = new Queue<>(
                "orders",
                PredefinedDataTypes.String,
                QueueOptions.builder()
                        // Only 1 attempt for this example, so the message goes to DLQ after the first failed processing
                        .defaultAttempts(1)
                        // Zero visibility timeout so the exhausted message becomes visible to sweep immediately.
                        // This is only for example purposes — in production it should be greater than zero for 99.9999% of cases.
                        .defaultVisibilityTimeout(Duration.ZERO)
                        .enableDlq(new DlqOptions(Duration.ofDays(14), 100_000L))
                        .build()
        );

        // Valid datasource from DI, static factory etc.
        var dataSource = ExamplesDataSourceProvider.getDataSource();

        // Update PostgreSQL schema.
        // This creates both the main queue table (q_orders) and the DLQ table (q_orders_dlq) automatically.
        SchemaHelpers.createOrUpdateQueues(dataSource, queue);

        // -------------------------------------------------------------------------------------------
        // Send a message to the queue
        var producer = new DatabaseProducer(dataSource);
        producer.send(queue, "Order #12345");

        // -------------------------------------------------------------------------------------------
        // Receive the message but do NOT delete it (simulating a processing failure).
        // After receiving, the message's remaining_attempts is decremented. Since we configured
        // defaultAttempts = 1, the message now has 0 remaining attempts and becomes "dead".
        var consumer = new DatabaseConsumer(dataSource);
        var message = consumer.receive(queue);
        System.out.println("Received from main queue: " + (message != null ? message.getData() : null));
        // Intentionally not calling consumer.delete() — the message processing "failed"

        // -------------------------------------------------------------------------------------------
        // Invoke sweep manually to move the dead message to the DLQ.
        // By default, sweep is probabilistic (every 10,000 receive/delete calls), which is efficient in production
        // but not suitable for a short example, so, for demonstration purposes, we invoke it manually here.
        // Sweep detects the dead message (remaining_attempts = 0) and moves it to the DLQ.
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            SweepHelper.sweep(connection, queue, 100);
            connection.commit();
        }

        // -------------------------------------------------------------------------------------------
        // Now we can inspect the DLQ. The dead letter queue is a regular Queue object, so it works
        // with all existing APIs — Consumer, Producer, Inspector, Mutator — without any special methods.
        var dlq = Objects.requireNonNull(queue.getDeadLetterQueue());
        var deadMessage = consumer.receive(dlq);
        System.out.println("Received from DLQ: " + (deadMessage != null ? deadMessage.getData() : null));

        // -------------------------------------------------------------------------------------------
        // Reprocess: send the failed message back to the main queue for another attempt
        if (deadMessage != null) {
            producer.send(queue, deadMessage.getData());
            consumer.delete(dlq, deadMessage);
            System.out.println("Message requeued for reprocessing");
        }

        // Verify the message is back in the main queue
        var reprocessed = consumer.receive(queue);
        System.out.println("Reprocessed from main queue: " + (reprocessed != null ? reprocessed.getData() : null));
        if (reprocessed != null) {
            consumer.delete(queue, reprocessed);
        }
    }
}
