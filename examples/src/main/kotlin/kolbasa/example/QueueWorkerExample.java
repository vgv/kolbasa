package kolbasa.example;

import kolbasa.consumer.datasource.DatabaseConsumer;
import kolbasa.consumer.worker.HandleResult;
import kolbasa.consumer.worker.QueueProcessor;
import kolbasa.consumer.worker.QueueWorker;
import kolbasa.consumer.worker.QueueWorkerOptions;
import kolbasa.producer.SendMessage;
import kolbasa.producer.datasource.DatabaseProducer;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.schema.SchemaHelpers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

class QueueWorkerExample {
    public static void main(String[] args) throws Exception {
        // Define queue with name `test_queue` and varchar type as data storage in PostgreSQL table
        var queue = Queue.of("test_queue", PredefinedDataTypes.String);

        // Valid datasource from DI, static factory etc.
        var dataSource = ExamplesDataSourceProvider.getDataSource();

        // Update PostgreSQL schema
        SchemaHelpers.createOrUpdateQueues(dataSource, queue);

        // -------------------------------------------------------------------------------------------
        // Send 1000 messages in one batch
        var producer = new DatabaseProducer(dataSource);
        var messages = new ArrayList<SendMessage<String>>();
        for (int i = 1; i <= 1000; i++) {
            messages.add(new SendMessage<>("Message " + i));
        }
        producer.send(queue, messages);

        // -------------------------------------------------------------------------------------------
        // Your business code for one message. Return DONE, and the message is deleted from the queue.
        // Throw, and it comes back after the visibility timeout for another attempt
        var handled = new CountDownLatch(1000);
        var perThread = new ConcurrentHashMap<String, AtomicInteger>();
        var processor = new QueueProcessor<>(new DatabaseConsumer(dataSource), queue, message -> {
            // ... your business code for message.getData() ...
            perThread.computeIfAbsent(Thread.currentThread().getName(), name -> new AtomicInteger()).incrementAndGet();
            handled.countDown();
            return HandleResult.DONE;
        });

        // -------------------------------------------------------------------------------------------
        // The worker calls the processor in 3 background threads, again and again. While the batches come full,
        // a thread takes the next one at once. When the queue is empty, it waits a bit and asks again
        var worker = new QueueWorker(processor, QueueWorkerOptions.builder().threads(3).build());
        worker.start();

        // A real application lets the worker run until the application stops. Here we wait for the 1000 messages only
        handled.await();

        // Stop the worker: the threads finish their batches and end. Its threads are not daemons, so without this
        // the program would never exit
        worker.stop(Duration.ofSeconds(10));

        // Both threads took their share of the batches
        new TreeMap<>(perThread).forEach((thread, count) ->
            System.out.println(thread + " handled " + count + " messages")
        );
    }
}
