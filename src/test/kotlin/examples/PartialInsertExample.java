package examples;

import kolbasa.producer.DeduplicationMode;
import kolbasa.producer.MessageResult;
import kolbasa.producer.PartialInsert;
import kolbasa.producer.SendMessage;
import kolbasa.producer.SendOptions;
import kolbasa.producer.SendRequest;
import kolbasa.producer.SendResult;
import kolbasa.producer.datasource.DatabaseProducer;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.queue.meta.FieldOption;
import kolbasa.queue.meta.MetaField;
import kolbasa.queue.meta.MetaValues;
import kolbasa.queue.meta.Metadata;
import kolbasa.schema.SchemaHelpers;

import java.util.List;

class PartialInsertExample {

    private static final MetaField<Integer> UNIQUE_KEY = MetaField.ofInt("unique_key", FieldOption.ALL_LIVE_UNIQUE);

    public static void main(String[] args) {
        // Define three queues to demonstrate different PartialInsert modes
        var queueProhibited =
                Queue.of("test_queue_prohibited", PredefinedDataTypes.String, Metadata.of(UNIQUE_KEY));
        var queueUntilFirstFailure =
                Queue.of("test_queue_until_first_failure", PredefinedDataTypes.String, Metadata.of(UNIQUE_KEY));
        var queueAsManyAsPossible =
                Queue.of("test_queue_as_many_as_possible", PredefinedDataTypes.String, Metadata.of(UNIQUE_KEY));

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
        SchemaHelpers.createOrUpdateQueues(dataSource, queueProhibited, queueUntilFirstFailure, queueAsManyAsPossible);

        // Messages to send with one poison message in the middle of the list
        // Due to different PartialInsert modes, the result of sending messages will be different
        var messagesToSend = List.of(
                new SendMessage<>("Unique key 1", MetaValues.of(UNIQUE_KEY.value(1))),
                new SendMessage<>("Unique key 2", MetaValues.of(UNIQUE_KEY.value(2))),
                new SendMessage<>("Unique key 3", MetaValues.of(UNIQUE_KEY.value(3))),
                new SendMessage<>("Unique key 1", MetaValues.of(UNIQUE_KEY.value(1))), // POISON MESSAGE
                new SendMessage<>("Unique key 5", MetaValues.of(UNIQUE_KEY.value(5))),
                new SendMessage<>("Unique key 6", MetaValues.of(UNIQUE_KEY.value(6)))
        );

        // -------------------------------------------------------------------------------------------
        // Create producer
        var producer = new DatabaseProducer(dataSource);

        // -------------------------------------------------------------------------------------------
        // PartialInsert mode: PROHIBITED
        // A poison message will cause an exception and all messages will be rejected, no messages will be sent to the queue.
        System.out.println("---------------------------------------------------------------------");
        System.out.println("Try to insert " + messagesToSend.size() + " messages, partial insert mode: " + PartialInsert.PROHIBITED);
        dumpResult(producer.send(
                queueProhibited,
                new SendRequest<>(
                        messagesToSend,
                        SendOptions.builder()
                                .partialInsert(PartialInsert.PROHIBITED)
                                .batchSize(2)
                                .deduplicationMode(DeduplicationMode.FAIL_ON_DUPLICATE)
                                .build()
                )
        ));

        // -------------------------------------------------------------------------------------------
        // PartialInsert mode: UNTIL_FIRST_FAILURE
        // A poison message will cause an exception, batch with this message and next batches will be rejected
        // Since we have 6 messages to send and batch size is 2, we will have 3 batches:
        // 1) First batch - success
        // 2) Second batch - error
        // 3) Third batch - error (don't even try to send, immediately mark it as rejected)
        System.out.println("---------------------------------------------------------------------");
        System.out.println("Try to insert " + messagesToSend.size() + " messages, partial insert mode: " + PartialInsert.UNTIL_FIRST_FAILURE);
        dumpResult(producer.send(
                queueUntilFirstFailure,
                new SendRequest<>(
                        messagesToSend,
                        SendOptions.builder()
                                .partialInsert(PartialInsert.UNTIL_FIRST_FAILURE)
                                .batchSize(2)
                                .deduplicationMode(DeduplicationMode.FAIL_ON_DUPLICATE)
                                .build()
                )
        ));

        // -------------------------------------------------------------------------------------------
        // PartialInsert mode: INSERT_AS_MANY_AS_POSSIBLE
        // A poison message will cause an exception, batch with this message will be rejected
        // Since we have 6 messages to send and batch size is 2, we will have 3 batches:
        // 1) First batch - success
        // 2) Second batch - error
        // 3) Third batch - success
        System.out.println("---------------------------------------------------------------------");
        System.out.println("Try to insert " + messagesToSend.size() + " messages, partial insert mode: " + PartialInsert.INSERT_AS_MANY_AS_POSSIBLE);
        dumpResult(producer.send(
                queueAsManyAsPossible,
                new SendRequest<>(
                        messagesToSend,
                        SendOptions.builder()
                                .partialInsert(PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)
                                .batchSize(2)
                                .deduplicationMode(DeduplicationMode.FAIL_ON_DUPLICATE)
                                .build()
                )
        ));
    }

    private static void dumpResult(SendResult<?> sendResult) {
        var failed = sendResult.onlyFailed().stream().mapToInt(error -> error.getMessages().size()).sum();
        System.out.println("OK: " + sendResult.onlySuccessful().size() + ", FAILURE: " + failed + ": ");

        for (var message : sendResult.getMessages()) {
            if (message instanceof MessageResult.Success<?> success) {
                System.out.println("Success: " + success.getMessage());
            } else if (message instanceof MessageResult.Duplicate<?> duplicate) {
                System.out.println("Duplicate: " + duplicate.getMessage());
            } else if (message instanceof MessageResult.Error<?> error) {
                System.out.println("Failed (" + error.getMessages().size() + "):");
                error.getMessages().forEach(failedMessage -> System.out.println("    Error: " + failedMessage));
            }
        }
    }
}
