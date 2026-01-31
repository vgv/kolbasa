package performance

import kolbasa.consumer.datasource.DatabaseConsumer
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

class ProducerConsumerTest : PerformanceTest {

    override fun run() {
        Env.ProducerConsumer.report()

        // Update
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        // Truncate table before test
        Env.Common.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        // Generate test data
        val randomData = (1..1000).map {
            val dataSize = (Env.ProducerConsumer.oneMessageSizeBytes * Random.nextDouble(0.9, 1.1)).toInt()
            Random.nextBytes(dataSize)
        }

        val sendCalls = AtomicLong()
        val receiveCalls = AtomicLong()

        val sendMessages = AtomicLong()
        val receiveMessages = AtomicLong()

        val producer = DatabaseProducer(Env.Common.dataSource, ProducerOptions(batchSize = Env.ProducerConsumer.batchSize))
        val consumer = DatabaseConsumer(Env.Common.dataSource)

        val producerThreads = (1..Env.ProducerConsumer.producerThreads).map {
            thread {
                while (sendCalls.get() < Env.ProducerConsumer.totalSendCalls) {
                    // Keep the baseline
                    while (sendMessages.get() > (receiveMessages.get() + Env.ProducerConsumer.queueSizeBaseline)) {
                        TimeUnit.MILLISECONDS.sleep(10)
                    }

                    if (sendCalls.incrementAndGet() <= Env.ProducerConsumer.totalSendCalls) {
                        val data = (1..Env.ProducerConsumer.oneSendMessages).map {
                            SendMessage(randomData.random())
                        }

                        producer.send(queue, data)
                            .throwExceptionIfAny(addOthersAsSuppressed = false)

                        sendMessages.addAndGet(data.size.toLong())
                    }
                }
            }
        }

        val consumerThreads = (1..Env.ProducerConsumer.consumerThreads).map {
            thread {
                while (receiveMessages.get() < Env.ProducerConsumer.totalSendCalls * Env.ProducerConsumer.oneSendMessages) {
                    val result = consumer.receive(queue, Env.ProducerConsumer.consumerReceiveLimit)
                    consumer.delete(queue, result)

                    // Increment
                    receiveMessages.addAndGet(result.size.toLong())
                    receiveCalls.incrementAndGet()
                }
            }
        }

        // Report stats
        thread {
            val start = System.currentTimeMillis()

            while (receiveMessages.get() < Env.ProducerConsumer.totalSendCalls * Env.ProducerConsumer.oneSendMessages) {
                TimeUnit.SECONDS.sleep(1)

                val seconds = ((System.currentTimeMillis() - start) / 1000)

                val produced = sendMessages.get()
                val produceRate = produced / seconds
                val sendCallsMade = sendCalls.get()
                val sendCallsRate = sendCallsMade / seconds

                val recv = receiveMessages.get()
                val recvRate = recv / seconds
                val recvCallsMade = receiveCalls.get()
                val recvCallsRate = recvCallsMade / seconds

                println("Time: $seconds s, sent: $produced msg ($produceRate msg/s), calls: $sendCalls ($sendCallsRate calls/s); received: $recv msg ($recvRate msg/s), calls: $recvCallsMade ($recvCallsRate calls/s)")

            }
        }

        (producerThreads + consumerThreads).forEach { it.join() }
    }

    companion object {
        private val queue = Queue.of(
            name = "producer_consumer_test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    ProducerConsumerTest().run()
}
