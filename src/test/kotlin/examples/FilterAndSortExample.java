package examples;

import kolbasa.consumer.ReceiveOptions;
import kolbasa.consumer.datasource.DatabaseConsumer;
import kolbasa.producer.SendMessage;
import kolbasa.producer.datasource.DatabaseProducer;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.queue.meta.FieldOption;
import kolbasa.queue.meta.MetaField;
import kolbasa.queue.meta.MetaValues;
import kolbasa.queue.meta.Metadata;
import kolbasa.schema.SchemaHelpers;

import java.util.ArrayList;
import java.util.List;

import static kolbasa.consumer.filter.Filter.eq;
import static kolbasa.consumer.filter.Filter.lessEq;
import static kolbasa.consumer.order.Order.desc;

class FilterAndSortExample {

    private static final MetaField<Integer> USER_ID = MetaField.ofInt("user_id", FieldOption.SEARCH);
    private static final MetaField<Integer> PRIORITY = MetaField.ofInt("priority", FieldOption.SEARCH);

    public static void main(String[] args) {
        // Define queue with name `test_queue`, varchar type as data storage and metadata
        var queue = Queue.of("test_queue", PredefinedDataTypes.String, Metadata.of(USER_ID, PRIORITY));

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
        // Create producer and send several messages with meta information
        var producer = new DatabaseProducer(dataSource);
        List<SendMessage<String>> messagesToSend = new ArrayList<>();
        for (int index = 1; index <= 100; index++) {
            var message = new SendMessage<>("Message " + index, MetaValues.of(USER_ID.value(index), PRIORITY.value(index % 10)));
            messagesToSend.add(message);
        }
        producer.send(queue, messagesToSend);


        // -------------------------------------------------------------------------------------------
        // Create consumer
        var consumer = new DatabaseConsumer(dataSource);

        // Try to read 100 messages with (userId<=10 or userId=78) from the queue and sort them by priority desc
        var receiveOptions = ReceiveOptions.builder()
                .readMetadata(true)
                .order(desc(PRIORITY))
                .filter(lessEq(USER_ID, 10).or(eq(USER_ID, 78)))
                .build();
        var messages = consumer.receive(queue, 100, receiveOptions);
        messages.forEach(System.out::println);
        // Delete all messages after processing
        consumer.delete(queue, messages);
    }
}
