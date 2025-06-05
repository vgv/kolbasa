package examples

import kolbasa.producer.*
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Unique
import kolbasa.schema.SchemaHelpers

fun main() {
    // User-defined class to store meta-information
    data class Metadata(
        @Unique val uniqueKey: Int
    )

    // Define three queues to demonstrate different PartialInsert modes
    val queueProhibited =
        Queue.of("test_queue_prohibited", PredefinedDataTypes.String, metadata = Metadata::class.java)
    val queueUntilFirstFailure =
        Queue.of("test_queue_until_first_failure", PredefinedDataTypes.String, metadata = Metadata::class.java)
    val queueAsManyAsPossible =
        Queue.of("test_queue_as_many_as_possible", PredefinedDataTypes.String, metadata = Metadata::class.java)

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
    SchemaHelpers.updateDatabaseSchema(dataSource, queueProhibited, queueUntilFirstFailure, queueAsManyAsPossible)

    // Messages to send with one poison message in the middle of the list
    // Due to different PartialInsert modes, the result of sending messages will be different
    val messagesToSend = listOf(
        SendMessage("Unique key 1", Metadata(1)),
        SendMessage("Unique key 2", Metadata(2)),
        SendMessage("Unique key 3", Metadata(3)),
        SendMessage("Unique key 1", Metadata(1)), // POISON MESSAGE
        SendMessage("Unique key 5", Metadata(5)),
        SendMessage("Unique key 6", Metadata(6)),
    )

    // -------------------------------------------------------------------------------------------
    // Create producer
    val producer = DatabaseProducer(dataSource)

    // -------------------------------------------------------------------------------------------
    // PartialInsert mode: PROHIBITED
    // A poison message will cause an exception and all messages will be rejected, no messages will be sent to the queue.
    println("---------------------------------------------------------------------")
    println("Try to insert ${messagesToSend.size} messages, partial insert mode: ${PartialInsert.PROHIBITED}")
    producer.send(
        queue = queueProhibited,
        request = SendRequest(
            data = messagesToSend,
            sendOptions = SendOptions(
                partialInsert = PartialInsert.PROHIBITED,
                batchSize = 2,
                deduplicationMode = DeduplicationMode.ERROR
            )
        )
    ).let { sendResult -> dumpResult(sendResult) }

    // -------------------------------------------------------------------------------------------
    // PartialInsert mode: UNTIL_FIRST_FAILURE
    // A poison message will cause an exception, batch with this message and next batches will be rejected
    // Since we have 6 messages to send and batch size is 2, we will have 3 batches:
    // 1) First batch - success
    // 2) Second batch - error
    // 3) Third batch - error (don't even try to send, immediately mark it as rejected)
    println("---------------------------------------------------------------------")
    println("Try to insert ${messagesToSend.size} messages, partial insert mode: ${PartialInsert.UNTIL_FIRST_FAILURE}")
    producer.send(
        queue = queueUntilFirstFailure,
        request = SendRequest(
            data = messagesToSend,
            sendOptions = SendOptions(
                partialInsert = PartialInsert.UNTIL_FIRST_FAILURE,
                batchSize = 2,
                deduplicationMode = DeduplicationMode.ERROR
            )
        )
    ).let { sendResult -> dumpResult(sendResult) }

    // -------------------------------------------------------------------------------------------
    // PartialInsert mode: INSERT_AS_MANY_AS_POSSIBLE
    // A poison message will cause an exception, batch with this message will be rejected
    // Since we have 6 messages to send and batch size is 2, we will have 3 batches:
    // 1) First batch - success
    // 2) Second batch - error
    // 3) Third batch - success
    println("---------------------------------------------------------------------")
    println("Try to insert ${messagesToSend.size} messages, partial insert mode: ${PartialInsert.INSERT_AS_MANY_AS_POSSIBLE}")
    producer.send(
        queue = queueAsManyAsPossible,
        request = SendRequest(
            data = messagesToSend,
            sendOptions = SendOptions(
                partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE,
                batchSize = 2,
                deduplicationMode = DeduplicationMode.ERROR
            )
        )
    ).let { sendResult ->
        dumpResult(sendResult)
    }
}

private fun dumpResult(sendResult: SendResult<*, *>) {
    println("OK: ${sendResult.onlySuccessful().size}, FAILURE: ${sendResult.onlyFailed().sumOf { it.messages.size }}: ")

    sendResult.messages.forEach { message ->
        when (message) {
            is MessageResult.Success -> {
                println("Success: ${message.message}")
            }

            is MessageResult.Duplicate -> {
                println("Duplicate: ${message.message}")
            }

            is MessageResult.Error -> {
                println("Failed (${message.messages.size}):")
                message.messages.forEach { failedMessage ->
                    println("    Error: $failedMessage")
                }
            }
        }
    }
}
