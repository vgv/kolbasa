package performance

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.utils.JdbcHelpers.useStatement
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.math.min
import kotlin.random.Random

class OnlyReceiveTest : PerformanceTest {

    override fun run() {
        Env.OnlyReceive.report()

        // Update schema
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        // Prepare test data
        run {
            val start = System.currentTimeMillis()
            println("Preparing test data...")

            // Truncate table before test
            Env.Common.dataSource.useStatement { statement ->
                statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
            }

            val randomData = (1..1000).map {
                val dataSize = (Env.OnlyReceive.oneMessageSizeBytes * Random.nextDouble(0.9, 1.1)).toInt()
                Random.nextBytes(dataSize)
            }

            val producer = DatabaseProducer(Env.Common.dataSource)
            var sentMessages = 0

            while (sentMessages < Env.OnlyReceive.messages) {
                val data = (1..min(1000, Env.OnlyReceive.messages - sentMessages)).map {
                    SendMessage(randomData.random())
                }

                producer.send(queue, data)
                    .throwExceptionIfAny(addOthersAsSuppressed = false)

                // Increment
                sentMessages += data.size
            }

            println("Prepared $sentMessages messages in ${(System.currentTimeMillis() - start) / 1000} seconds")
        }

        val receivedMessages = AtomicLong(0)
        val receiveCalls = AtomicLong()

        val consumer = DatabaseConsumer(Env.Common.dataSource)

        val consumerThreads = (1..Env.OnlyReceive.threads).map {
            thread {
                while (receivedMessages.get() < Env.OnlyReceive.messages) {
                    val result = consumer.receive(queue, limit = Env.OnlyReceive.consumerReceiveLimit)
                    receivedMessages.addAndGet(result.size.toLong())
                    receiveCalls.incrementAndGet()
                }
            }
        }

        // Report stats
        thread {
            val start = System.currentTimeMillis()

            while (receivedMessages.get() < Env.OnlyReceive.messages) {
                TimeUnit.SECONDS.sleep(1)

                val seconds = ((System.currentTimeMillis() - start) / 1000)

                val recvMessages = receivedMessages.get()
                val recvRate = recvMessages / seconds

                val recvCallsMade = receiveCalls.get()
                val recvCallsRate = recvCallsMade / seconds

                println("Time: $seconds s, received: $recvMessages msg ($recvRate msg/s), calls: $recvCallsMade ($recvCallsRate calls/s)")
                println("-------------------------------------------")
            }
        }

        consumerThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue.of(
            name = "only_receive_test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    OnlySendTest().run()
}
