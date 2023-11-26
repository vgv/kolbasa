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

class ProducerTest : PerformanceTest {

    override fun run() {
        Env.reportProducerTestEnv()

        // Update
        SchemaHelpers.updateDatabaseSchema(Env.dataSource, queue)

        // Generate data
        val randomData = (1..1000).map {
            val dataSize = Env.producerTestDataSizeBytes + Random.nextInt(-10, 10)
            Random.nextBytes(dataSize)
        }

        val producedRecords = AtomicLong()

        val producerThreads = (1..Env.producerTestThreads).map {
            thread {
                val producer = DatabaseProducer(Env.dataSource, queue)
                while (producedRecords.get() < Env.producerTestIterations) {
                    val data = (1..Env.producerTestSendSize).map {
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

            while (producedRecords.get() < Env.producerTestIterations) {
                TimeUnit.SECONDS.sleep(1)
                val current = producedRecords.get()
                println("Produced: ${current - lastRecords} items/sec")
                lastRecords = current
            }
        }

        // Truncate table
        thread {
            while (producedRecords.get() < Env.producerTestIterations) {
                TimeUnit.SECONDS.sleep(1)
                Env.dataSource.useStatement { statement ->
                    statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
                }
            }
        }

        producerThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue<ByteArray, Unit>(
            name = "producer_test",
            databaseDataType = PredefinedDataTypes.ByteArray,
            metadata = null
        )
    }
}
