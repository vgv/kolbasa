package kolbasa.example;

import kolbasa.consumer.datasource.DatabaseConsumer;
import kolbasa.producer.SendMessage;
import kolbasa.producer.datasource.DatabaseProducer;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.schema.SchemaHelpers;
import kolbasa.worker.*;
import kolbasa.worker.pipe.*;

import java.util.ArrayList;

class PipeExample {

    public static void main(String[] args) throws Exception {
        // Imitate two PostgreSQL servers
        var sourceDataSource = ExamplesDataSourceProvider.getDataSource();
        var targetDataSource = ExamplesDataSourceProvider.getDataSource();

        // Orders come to the source queue as plain text. The target queue gets them in upper case
        var sourceQueue = Queue.of("orders", PredefinedDataTypes.String);
        var targetQueue = Queue.of("orders_copy", PredefinedDataTypes.String);

        // Update PostgreSQL schema, every queue on its own server
        SchemaHelpers.createOrUpdateQueues(sourceDataSource, sourceQueue);
        SchemaHelpers.createOrUpdateQueues(targetDataSource, targetQueue);

        // -------------------------------------------------------------------------------------------
        // Send 25 orders to the source queue
        var orders = new ArrayList<SendMessage<String>>();
        for (int i = 1; i <= 25; i++) {
            orders.add(new SendMessage<>("order " + i));
        }
        new DatabaseProducer(sourceDataSource).send(sourceQueue, orders);

        // -------------------------------------------------------------------------------------------
        // The pipe takes a batch from the source queue, turns every message into one message for the target queue,
        // sends the whole batch in one call and deletes it from the source queue. Run it next to the source queue:
        // then only the send goes the long way.
        // If you don't need to change the messages, use QueuePipe.asIs: it moves them without even reading the data
        var pipe = QueuePipe.map(
            sourceQueue,
            new DatabaseConsumer(sourceDataSource),
            targetQueue,
            new DatabaseProducer(targetDataSource),
            message -> new SendMessage<>(message.getData().toUpperCase())
        );

        // Every process() call moves one batch: receive, transform, one send, one delete. Here we call it until the
        // source queue is empty. A real application gives the pipe to a QueueWorker instead, like a QueueProcessor in
        // QueueWorkerExample: new QueueWorker(pipe).start()
        ProcessResult result;
        do {
            result = pipe.process();
            System.out.println("Moved " + result.getSucceeded() + " orders");
        } while (result.isBatchFull());

        // -------------------------------------------------------------------------------------------
        // The orders are on the target server now
        var moved = new DatabaseConsumer(targetDataSource).receive(targetQueue, 100);
        var example = moved.get(0).getData();
        System.out.println("Target queue has " + moved.size() + " orders, for example '" + example + "'");
    }
}
