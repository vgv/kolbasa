package performance

import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.ProducerOptions
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
            val dataSize = (Env.pDataSizeBytes * Random.nextDouble(0.9, 1.1)).toInt()

            Random.nextBytes(dataSize)
        }

        val producedRecords = AtomicLong()

        val producerThreads = (1..Env.pThreads).map {
            thread {
                val producer = DatabaseProducer(Env.dataSource, ProducerOptions(batchSize = Env.pBatchSize))

                while (true) {
                    val data = (1..Env.pSendSize).map {
                        SendMessage<ByteArray, Unit>(randomData.random())
                    }

                    val result = producer.send(queue, data)
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
            val start = System.currentTimeMillis()

            while (true) {
                TimeUnit.SECONDS.sleep(1)
                val currentProducer = producedRecords.get() / ((System.currentTimeMillis() - start) / 1000)
                val seconds = ((System.currentTimeMillis() - start) / 1000)
                println("Seconds: $seconds, produced: $currentProducer items/sec")
                println("-------------------------------------------")
            }
        }

        // Truncate table
        thread {
            while (true) {
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

fun main() {
    ProducerTest().run()
}
