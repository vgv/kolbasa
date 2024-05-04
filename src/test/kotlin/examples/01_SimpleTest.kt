package examples

import kolbasa.consumer.DatabaseConsumer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers

fun main() {
    // Define queue with name `test_queue` and varchar type as data storage in PostgreSQL table
    val queue = Queue("test_queue", PredefinedDataTypes.String, metadata = Unit::class.java)

    // Valid datasource from DI, static factory etc.
    val dataSource = ExamplesDataSourceProvider.getDataSource()

    // Update PostgreSQL schema
    // We need to create (or update) queue table before send/receive
    SchemaHelpers.updateDatabaseSchema(dataSource, queue)

    // Create producer and send simple message
    val producer = DatabaseProducer(dataSource, queue)
    producer.send("Test message")

    // Create consumer, try to read message from the queue, process it and delete
    val consumer = DatabaseConsumer(dataSource, queue)
    consumer.receive()?.let { message ->
        println(message.data)
        consumer.delete(message)
    }
}
