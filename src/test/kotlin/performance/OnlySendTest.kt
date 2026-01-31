package performance

import kolbasa.utils.JdbcHelpers.useStatement
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

class OnlySendTest : PerformanceTest {

    override fun run() {
        Env.OnlySend.report()

        // Update schema
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        // Generate data
        val randomData = (1..1000).map {
            val dataSize = (Env.OnlySend.oneMessageSizeBytes * Random.nextDouble(0.9, 1.1)).toInt()
            Random.nextBytes(dataSize)
        }

        val calls = AtomicLong()
        val messages = AtomicLong()

        val producer = DatabaseProducer(Env.Common.dataSource, ProducerOptions(batchSize = Env.OnlySend.batchSize))

        val producerThreads = (1..Env.OnlySend.threads).map {
            thread {
                while (calls.incrementAndGet() <= Env.OnlySend.totalSendCalls) {
                    val data = (1..Env.OnlySend.oneSendMessages).map {
                        SendMessage(randomData.random())
                    }

                    producer.send(queue, data)
                        .throwExceptionIfAny(addOthersAsSuppressed = false)

                    // Increment
                    messages.addAndGet(data.size.toLong())
                }
            }
        }

        // Report stats
        thread {
            val start = System.currentTimeMillis()

            while (calls.get() < Env.OnlySend.totalSendCalls) {
                TimeUnit.SECONDS.sleep(1)

                val seconds = ((System.currentTimeMillis() - start) / 1000)

                val produced = messages.get()
                val produceRate = produced / seconds

                val callsMade = calls.get()
                val callsRate = callsMade / seconds

                println("Time: $seconds s, sent: $produced msg ($produceRate msg/s), calls: $callsMade ($callsRate calls/s)")
                println("-------------------------------------------")
            }
        }

        // Truncate table
        thread {
            while (calls.get() < Env.OnlySend.totalSendCalls) {
                Env.Common.dataSource.useStatement { statement ->
                    statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
                }

                TimeUnit.SECONDS.sleep(1)
            }
        }

        producerThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue.of(
            name = "only_send_test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    OnlySendTest().run()
}
