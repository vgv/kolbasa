package examples

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.DeduplicationMode
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers

private val USER_ID = MetaField.int("user_id", FieldOption.STRICT_UNIQUE)

fun main() {
    // Define queue with name `test_queue`, varchar type as data storage and metadata
    val queue = Queue.of("test_queue", PredefinedDataTypes.String, metadata = Metadata.of(USER_ID))

    // Valid datasource from DI, static factory etc.
    val dataSource = ExamplesDataSourceProvider.getDataSource()

    // Update PostgreSQL schema
    // We need to create (or update) the queue table before the first use, since the table schema can be changed - for
    // example, new meta fields were added or other internal schema changes occurred. This is a convenient method that allows
    // you not to think about whether this queue has been used before or this is the first time and simply brings its state
    // in the database to the current one.
    // Of course, in a real application this should be done once at the start of the service, and not before each send/receive.
    // A good analogy is updating the business tables schema before the start of the service using migration or other
    // methods - this should be done once at the start of the service, and not before each SQL query from these tables.
    SchemaHelpers.updateDatabaseSchema(dataSource, queue)

    // -------------------------------------------------------------------------------------------
    // Create producer and send 10 messages with meta information with duplicated userId field.
    // By default, the deduplication mode is set to ERROR, so, if you try to send a message with the existing
    // userId, an exception will be thrown.
    // However, depending on your task, it may be useful to simply ignore these errors and insert only those messages
    // where there are no duplicates. There is a mode of operation for this IGNORE_DUPLICATES
    // Here we will use this mode and create 10 messages with only 5 unique userId values and make sure that send() call
    // does not throw an exception.
    val producer = DatabaseProducer(dataSource)
    val messagesToSend = (1..10).map { index ->
        val userId = index % 5
        SendMessage("Message $index, userId=$userId", MetaValues.of(USER_ID.value(userId)))
    }
    producer.send(
        queue = queue,
        request = SendRequest(
            data = messagesToSend,
            sendOptions = SendOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATES)
        )
    )

    println("------------------------------------------------------")
    println("Sent messages (${messagesToSend.size}): ")
    messagesToSend.forEach { message ->
        println("  $message")
    }


    // -------------------------------------------------------------------------------------------
    // Create consumer
    val consumer = DatabaseConsumer(dataSource)
    // Read all messages from the queue
    // Expected result: only 5 messages with unique userId values
    val messages = consumer.receive(queue, 100)
    println("------------------------------------------------------")
    println("Received messages (${messages.size}): ")
    messages.forEach { message ->
        println("  $message")
    }
    // Delete all messages after processing
    consumer.delete(queue, messages)
}

