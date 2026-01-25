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
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        // Truncate table before test
        Env.Common.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        // Generate test data
        val randomData = (1..1000).map {
            val dataSize = (Env.ProducerConsumer.dataSizeBytes * Random.nextDouble(0.9, 1.1)).toInt()
            Random.nextBytes(dataSize)
        }

        val producedRecords = AtomicLong()
        val consumedRecords = AtomicLong()

        val producer = DatabaseProducer(Env.Common.dataSource, ProducerOptions(batchSize = Env.ProducerConsumer.batchSize))
        val consumer = DatabaseConsumer(Env.Common.dataSource)

        val producerThreads = (1..Env.ProducerConsumer.producerThreads).map {
            thread {
                while (true) {
                    val produced = producedRecords.get()
                    val consumed = consumedRecords.get()

                    if (produced > consumed + Env.ProducerConsumer.queueSizeBaseline) {
                        TimeUnit.MILLISECONDS.sleep(100)
                    } else {
                        val data = (1..Env.ProducerConsumer.sendSize).map {
                            SendMessage(randomData.random())
                        }

                        producer.send(queue, data)
                            .throwExceptionIfAny(addOthersAsSuppressed = false)

                        // Increment
                        producedRecords.addAndGet(data.size.toLong())
                    }
                }
            }
        }

        val consumerThreads = (1..Env.ProducerConsumer.consumerThreads).map {
            thread {
                while (true) {
                    val result = consumer.receive(queue, Env.ProducerConsumer.consumerReceiveLimit)
                    consumer.delete(queue, result)

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
        private val queue = Queue.of(
            name = "producer_consumer_test",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    ProducerConsumerTest().run()
}
