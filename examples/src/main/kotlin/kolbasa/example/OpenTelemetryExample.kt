package kolbasa.example

import kolbasa.Kolbasa
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.opentelemetry.OpenTelemetryTracing
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
    SchemaHelpers.createOrUpdateQueues(dataSource, queue)

    // Valid OpenTelemetry from DI, the OpenTelemetry agent etc.
    // This one prints every finished span to the console, so you can see what Kolbasa does
    val openTelemetry = ExamplesOpenTelemetryProvider.getOpenTelemetry()

    // Turn on tracing for every producer and consumer, once at the start of the application
    Kolbasa.tracing = OpenTelemetryTracing(openTelemetry)

    // -------------------------------------------------------------------------------------------
    // send() makes a producer span. Its trace context is stored in the database together with the message
    val producer = DatabaseProducer(dataSource)
    producer.send(queue, "Test message")

    // -------------------------------------------------------------------------------------------
    // receive() makes a consumer span that links to the producer span: the trace context came back with the
    // message. So both sides can be seen as one flow, even if they are different applications
    val consumer = DatabaseConsumer(dataSource)
    consumer.receive(queue)?.let { message ->
        println(message.data)
        consumer.delete(queue, message)
    }
}
