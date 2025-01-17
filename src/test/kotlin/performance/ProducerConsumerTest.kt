package performance

import kolbasa.consumer.datasource.DatabaseConsumer
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

class ProducerConsumerTest : PerformanceTest {

    override fun run() {
        Env.reportProducerConsumerTestEnv()

        // Update
        SchemaHelpers.updateDatabaseSchema(Env.dataSource, queue)

        // Truncate table before test
        Env.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        // Generate test data
        val randomData = (1..1000).map {
            val dataSize = (Env.pcDataSizeBytes * Random.nextDouble(0.9, 1.1)).toInt()
            Random.nextBytes(dataSize)
        }

        val producedRecords = AtomicLong()
        val consumedRecords = AtomicLong()

        val producerThreads = (1..Env.pcProducerThreads).map {
            thread {
                val producer = DatabaseProducer(Env.dataSource, ProducerOptions(batchSize = Env.pcBatchSize))
                while (true) {
                    val produced = producedRecords.get()
                    val consumed = consumedRecords.get()

                    if (produced > consumed + Env.pcQueueSizeBaseline) {
                        TimeUnit.MILLISECONDS.sleep(100)
                    } else {
                        val data = (1..Env.pcSendSize).map {
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
        }

        val consumerThreads = (1..Env.pcConsumerThreads).map {
            thread {
                val consumer = DatabaseConsumer(Env.dataSource, queue)
                while (true) {
                    val result = consumer.receive(Env.pcConsumerReceiveLimit)
                    consumer.delete(result)

                    // Increment
                    consumedRecords.addAndGet(result.size.toLong())
                }
            }
        }

        // Report stats
        thread {
            val start = System.currentTimeMillis()

            TimeUnit.SECONDS.sleep(10)
            while (true) {
                TimeUnit.SECONDS.sleep(1)
                val currentProducer = producedRecords.get() / ((System.currentTimeMillis() - start) / 1000)
                val currentConsumer = consumedRecords.get() / ((System.currentTimeMillis() - start) / 1000)
                val seconds = ((System.currentTimeMillis() - start) / 1000)
                println("Seconds: $seconds, produced: $currentProducer items/sec, consumed: $currentConsumer items/sec")
                println("-------------------------------------------")
            }
        }

        (producerThreads + consumerThreads).forEach { it.join() }
    }

    companion object {
        private val queue = Queue<ByteArray, Unit>(
            name = "producer_consumer_test",
            databaseDataType = PredefinedDataTypes.ByteArray,
            metadata = null
        )
    }
}

fun main() {
    ProducerConsumerTest().run()
}
