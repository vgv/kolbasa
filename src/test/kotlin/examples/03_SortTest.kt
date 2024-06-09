package examples

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.lessEq
import kolbasa.consumer.filter.Filter.or
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.SendMessage
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Searchable
import kolbasa.schema.SchemaHelpers

fun main() {
    // User-defined class to store meta-information
    data class Metadata(
        @Searchable val userId: Int,
        @Searchable val priority: Int
    )

    // Define queue with name `test_queue`, varchar type as data storage and metadata
    val queue = Queue("test_queue", PredefinedDataTypes.String, metadata = Metadata::class.java)

    val dataSource = ExamplesDataSourceProvider.getDataSource()

    // Update PostgreSQL schema
    // We need to create (or update) queue table before send/receive
    SchemaHelpers.updateDatabaseSchema(dataSource, queue)

    // Create producer and send several messages with meta information
    val producer = DatabaseProducer(dataSource, queue)
    val messagesToSend = (1..100).map { index ->
        SendMessage("Message $index", Metadata(userId = index, priority = index % 10))
    }
    producer.send(messagesToSend)


    // Create consumer
    val consumer = DatabaseConsumer(dataSource, queue)

    // Try to read 100 messages with (userId<=10 or userId=78) from the queue and sort them by priority desc
    val receiveOptions = ReceiveOptions(
        readMetadata = true,
        order = Metadata::priority.desc(),
        filter = (Metadata::userId lessEq 10) or (Metadata::userId eq 78)
    )
    val messages = consumer.receive(limit = 100, receiveOptions)
    messages.forEach {
        println(it)
    }
    // Delete all messages after processing
    consumer.delete(messages)
}

