package examples

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers

fun main() {
    // Define queue with name `test_queue` and varchar type as data storage in PostgreSQL table
    val queue = Queue.of("test_queue", PredefinedDataTypes.String)

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
    SchemaHelpers.createOrUpdateQueues(dataSource, queue)

    // -------------------------------------------------------------------------------------------
    // Create producer and send simple message
    val producer = DatabaseProducer(dataSource)
    producer.send(queue, "Test message")

    // -------------------------------------------------------------------------------------------
    // Create consumer, try to read message from the queue, process it and delete
    val consumer = DatabaseConsumer(dataSource)
    consumer.receive(queue)?.let { message ->
        println(message.data)
        consumer.delete(queue, message)
    }
}
