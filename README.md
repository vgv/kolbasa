# kolbasa

Kolbasa is a small, efficient and capable Kotlin library to add PostgreSQL-based queues to your project.

## Features
* PostgreSQL as a persistent storage
* Message deduplication
* Message send delay (initial delay before message will be visible to consumers)
* Message visibility timeout (delay before consumed but not deleted message will be visible to another consumers)
* Configurable amount of receive attempts
* Ability to receive messages filtered by one or more meta-fields (like `user_id=42 and event_type=PAGE_VIEW`)
* Ability to receive messages sorted by one or more meta-fields (like `custom_priority desc, created asc`)
* Supports working in "external" transaction context (send/receive messages from a queue will follow "external" transaction commit/rollback)
* Batch send/receive to improve performance
* Different modes to deal with sending failures (fail all messages in a batch, send all until first failure, send as many as possible)
* Share load between different PostgreSQL servers

## Concepts
Kolbasa is a pure Kotlin library, so it can be used with any JVM language (Java, Kotlin, Scala etc.).

Kolbasa uses PostgreSQL as a storage to manage all queues, store all messages, ensure ACID and allow filtering and sorting. Kolbasa doesn't require any special PostgreSQL plugins or specific compile/runtime settings. It works on plain PostgreSQL version 10 and above.

## Requirements
* PostgreSQL 10+
* JVM 17+


## How to add Kolbasa into your project
### Gradle
```groovy
implementation "io.github.vgv:kolbasa:0.49.0"
```
### Maven
```xml
<dependency>
    <groupId>io.github.vgv</groupId>
    <artifactId>kolbasa</artifactId>
    <version>0.49.0</version>
</dependency>
```

## Examples
The easiest way to try kolbasa is to try running real, working examples, illustrating different features and modes. All examples can be found in the [examples](src/test/kotlin/examples) folder. Each example is a ready to run, complete mini-program that can be launched from the IDE or Gradle. To run from Gradle, you need to execute the command `./gradlew example -P example_name=01_SimpleTest`, where `example_name` is the name of the file with the example.

You need to have a working PostgreSQL instance to run examples and here you have two options:
1) Easiest (default) – just have running Docker on your machine. All examples will use Docker to start PostgreSQL instance.
2) If you don't want to or can't use Docker, you have a second option – use a real PostgreSQL installation. File [ExamplesDataSourceProvider](src/test/kotlin/examples/ExamplesDataSourceProvider.kt) is the place where you can specify url, username and password for you existing PostgreSQL instance.

### Simple example
The simplest possible example to send and receive one simple message: [SimpleExample](src/test/kotlin/examples/SimpleExample.kt)

### Filtering and sorting
What if every message is associated with additional, user-defined meta-data such as `userId` and `priority` (for example) and we want to receive messages with a specific userId and sort them by `priority`? Kolbasa can receive only particular messages from queue using convenient type-safe DSL and order them.

First, let's look at filtering
```kotlin
// User-defined class to store meta-information
data class Metadata(
    @Searchable val userId: Int,
    @Searchable val priority: Int
)

// Define queue with name `test_queue`, varchar type as data storage and metadata
val queue = Queue("test_queue", PredefinedDataTypes.String, metadata = Metadata::class.java)

val dataSource = ... // Valid datasource from DI, static factory etc.

// Update PostgreSQL schema
// We need to create (or update) queue table before send/receive
SchemaHelpers.updateDatabaseSchema(dataSource, queue)

// Create producer and send several messages with meta information
val producer = DatabaseProducer(dataSource, queue)
producer.send(SendMessage("First message", Metadata(userId = 1, priority = 10)))
producer.send(SendMessage("Second message", Metadata(userId = 2, priority = 1)))

// Create consumer
val consumer = DatabaseConsumer(dataSource, queue)
// Try to read 100 messages with userId=1 from the queue
val messages = consumer.receive(100) {
    Metadata::userId eq 1  // Type-safe DSL to filter messages
}
messages.forEach {  /* process messages */ }
// Delete all messages after processing
consumer.delete(messages)
```


Second, let's add sorting here
```kotlin
val receiveOptions = ReceiveOptions(
    order = Metadata::priority.desc(), // order by priority desc
    filter = Metadata::userId eq 1 // ... and filter by userId
)
// Try to read 100 messages with userId=1 and `priority desc` sorting from the queue
val messages = consumer.receive(100, receiveOptions)
```
