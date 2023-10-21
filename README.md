# kolbasa

Kolbasa is a small, efficient and capable Kotlin library to add Postgresql-based queues to your project. 


## How to add Kolbasa into your project
### Gradle
```groovy
compile "io.github.vgv:kolbasa:0.10.0"
```
### Maven
```xml
<dependency>
    <groupId>io.github.vgv</groupId>
    <artifactId>kolbasa</artifactId>
    <version>0.10.0</version>
</dependency>
```

## How to use
### Simple example
```kotlin
// Define queue with name `test_queue` and varchar type as data storage
val queue = Queue("test_queue", PredefinedDataTypes.String, metadata = Unit::class.java)

val dataSource = ... // Valid datasource from DI, static factory etc.

// Update Postgresql schema, if there were changes
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
```
