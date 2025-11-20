package examples

import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.lessEq
import kolbasa.consumer.filter.Filter.or
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers

private val USER_ID = MetaField.int("user_id", FieldOption.SEARCHABLE)
private val PRIORITY = MetaField.int("priority", FieldOption.SEARCHABLE)

fun main() {
    // Define queue with name `test_queue`, varchar type as data storage and metadata
    val queue = Queue.of("test_queue", PredefinedDataTypes.String, metadata = Metadata.of(USER_ID, PRIORITY))

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
    // Create producer and send several messages with meta information
    val producer = DatabaseProducer(dataSource)
    val messagesToSend = (1..100).map { index ->
        SendMessage("Message $index", MetaValues.of(USER_ID.value(index), PRIORITY.value(index % 10)))
    }
    producer.send(queue, messagesToSend)


    // -------------------------------------------------------------------------------------------
    // Create consumer
    val consumer = DatabaseConsumer(dataSource)

    // Try to read 100 messages with (userId<=10 or userId=78) from the queue and sort them by priority desc
    val receiveOptions = ReceiveOptions(
        readMetadata = true,
        order = PRIORITY.desc(),
        filter = (USER_ID lessEq 10) or (USER_ID eq 78)
    )
    val messages = consumer.receive(queue, limit = 100, receiveOptions)
    messages.forEach {
        println(it)
    }
    // Delete all messages after processing
    consumer.delete(queue, messages)
}

