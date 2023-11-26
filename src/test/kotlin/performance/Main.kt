package performance

import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.DatabaseProducer
import kolbasa.producer.SendMessage
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.random.Random

private const val DEFAULT_THREADS = 1
private const val DEFAULT_ITERATIONS = 10_000_000
private const val DEFAULT_PRODUCER_SEND_SIZE = 1_000
private const val DEFAULT_PRODUCER_BATCH_SIZE = 500
private const val DEFAULT_DATA_SIZE_BYTES = 500

private val queue = Queue<ByteArray, Unit>("producer_test", PredefinedDataTypes.ByteArray, metadata = null)

fun main() {
    val threads = System.getenv("threads")?.toIntOrNull() ?: DEFAULT_THREADS
    val iterations = System.getenv("iterations")?.toIntOrNull() ?: DEFAULT_ITERATIONS
    val producerSendSize = System.getenv("send_size")?.toIntOrNull() ?: DEFAULT_PRODUCER_SEND_SIZE
    val producerBatchSize = System.getenv("batch_size")?.toIntOrNull() ?: DEFAULT_PRODUCER_BATCH_SIZE
    val dataSize = System.getenv("data_size")?.toIntOrNull() ?: DEFAULT_DATA_SIZE_BYTES
    val dataSource = if ("external" == System.getenv("datasource")) {
        PerformanceDataSourceProvider.externalDatasource()
    } else {
        PerformanceDataSourceProvider.internalDatasource()
    }

    println("--------------------------------------------------")
    println("Threads: $threads")
    println("Iterations: $iterations")
    println("Producer send size: $producerSendSize")
    println("Producer batch size: $producerBatchSize")
    println("Data size: $dataSize")
    println("--------------------------------------------------")

    // Update
    SchemaHelpers.updateDatabaseSchema(dataSource, queue)

    // Pregenerate data
    val randomData = (1..1000).map {
        Random.nextBytes(dataSize)
    }

    val producedRecords = AtomicLong()

    val producerThreads = (1..threads).map {
        thread {
            val producer = DatabaseProducer(dataSource, queue)
            while (producedRecords.get() < iterations) {
                val data = (1..producerSendSize).map {
                    SendMessage<ByteArray, Unit>(randomData.random())
                }

                val result = producer.send(data)
                if (result.failedMessages > 0) {
                    // Just throw first exception and finish
                    throw result.onlyFailed().first().exception
                }

                // Increment
                producedRecords.addAndGet(data.size.toLong())
            }
        }
    }

    // Report stats
    thread {
        var lastRecords = 0L

        while (producedRecords.get() < iterations) {
            TimeUnit.SECONDS.sleep(1)
            val current = producedRecords.get()
            println("Produced: ${current - lastRecords} items/sec")
            lastRecords = current
        }
    }

    // Truncate table
    thread {
        while (producedRecords.get() < iterations) {
            TimeUnit.SECONDS.sleep(1)
            dataSource.useStatement { statement ->
                statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
            }
        }
    }

    producerThreads.forEach { it.join() }
}
