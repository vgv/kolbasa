package examples;

import kolbasa.consumer.Message;
import kolbasa.consumer.datasource.DatabaseConsumer;
import kolbasa.producer.MessageOptions;
import kolbasa.producer.SendMessage;
import kolbasa.producer.datasource.DatabaseProducer;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.queue.meta.MetaValues;
import kolbasa.schema.SchemaHelpers;

import java.time.Duration;

class SendDelayExample {
    public static void main(String[] args) throws InterruptedException {
        // Define queue with name `test_queue` and varchar type as data storage in PostgreSQL table
        var queue = Queue.of("test_queue", PredefinedDataTypes.String);

        // Valid datasource from DI, static factory etc.
        var dataSource = ExamplesDataSourceProvider.getDataSource();

        // Update PostgreSQL schema
        // We need to create (or update) the queue table before the first use, since the table schema can be changed - for
        // example, new meta fields were added or other internal schema changes occurred. This is a convenient method that allows
        // you not to think about whether this queue has been used before or this is the first time and simply brings its state
        // in the database to the current one.
        // Of course, in a real application this should be done once at the start of the service, and not before each send/receive.
        // A good analogy is updating the business tables schema before the start of the service using migration or other
        // methods - this should be done once at the start of the service, and not before each SQL query from these tables.
        SchemaHelpers.createOrUpdateQueues(dataSource, queue);

        // -------------------------------------------------------------------------------------------
        // Create producer and send simple message
        var seconds = 5L;
        System.out.println("Send a message with a " + seconds + "-second initial delay");
        var producer = new DatabaseProducer(dataSource);
        var sendMessage = new SendMessage<>(
                "Test message",
                MetaValues.EMPTY,
                MessageOptions.builder().delay(Duration.ofSeconds(seconds)).build()
        );
        producer.send(queue, sendMessage);

        // -------------------------------------------------------------------------------------------
        // Create consumer, try to read message from the queue, process it and delete
        var consumer = new DatabaseConsumer(dataSource);
        Message<String> message;
        do {
            message = consumer.receive(queue);
            if (message == null) {
                // Sleep for 1 second before next attempt
                System.out.println("Message not found, waiting...");
                Thread.sleep(1000);
            } else {
                System.out.println("Message received: " + message);
                consumer.delete(queue, message);
            }
        } while (message == null);
    }
}
