package performance

import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.random.Random

class OnlyProducerTest : PerformanceTest {

    override fun run() {
        Env.reportOnlyProducerTestEnv()

        // Update schema
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        // Generate data
        val randomData = (1..1000).map {
            val dataSize = (Env.OnlyProducer.dataSizeBytes * Random.nextDouble(0.9, 1.1)).toInt()
            Random.nextBytes(dataSize)
        }

        val producedRecords = AtomicLong()

        val producer = DatabaseProducer(Env.Common.dataSource, ProducerOptions(batchSize = Env.OnlyProducer.batchSize))

        val producerThreads = (1..Env.OnlyProducer.threads).map {
            thread {
                while (true) {
                    val data = (1..Env.OnlyProducer.sendSize).map {
                        SendMessage(randomData.random())
                    }

                    producer.send(queue, data)
                        .throwExceptionIfAny(addOthersAsSuppressed = false)

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

                val seconds = ((System.currentTimeMillis() - start) / 1000)
                val currentProducer = producedRecords.get() / seconds

                println("Seconds: $seconds, produced: $currentProducer items/sec")
                println("-------------------------------------------")
            }
        }

        // Truncate table
        thread {
            while (true) {
                TimeUnit.SECONDS.sleep(1)
                Env.Common.dataSource.useStatement { statement ->
                    statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
                }
            }
        }

        producerThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue.of(
            name = "producer_test",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    OnlyProducerTest().run()
}
