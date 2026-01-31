package performance

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class EmptyReceiveTest : PerformanceTest {

    override fun run() {
        Env.EmptyReceive.report()

        // Update
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        // Truncate table before test
        Env.Common.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        val receiveCalls = AtomicLong()

        val consumer = DatabaseConsumer(Env.Common.dataSource)

        val consumerThreads = (1..Env.EmptyReceive.threads).map {
            thread {
                while (receiveCalls.incrementAndGet() <= Env.EmptyReceive.totalReceiveCalls) {
                    consumer.receive(queue)
                }
            }
        }

        // Report stats
        thread {
            val start = System.currentTimeMillis()

            while (receiveCalls.get() < Env.EmptyReceive.totalReceiveCalls) {
                TimeUnit.SECONDS.sleep(1)

                val seconds = ((System.currentTimeMillis() - start) / 1000)

                val recvCalls = receiveCalls.get()
                val receiveRate = recvCalls / seconds

                println("Time: $seconds s, receive calls: $recvCalls ($receiveRate calls/s)")
                println("-------------------------------------------")
            }
        }

        consumerThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue.of(
            name = "empty_receive_test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    EmptyReceiveTest().run()
}
