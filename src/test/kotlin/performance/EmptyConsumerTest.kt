package performance

import kolbasa.consumer.DatabaseConsumer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class EmptyConsumerTest: PerformanceTest {

    override fun run() {
        Env.reportEmptyConsumerTestEnv()

        // Update
        SchemaHelpers.updateDatabaseSchema(Env.dataSource, queue)

        val consumeCalls = AtomicLong()

        val consumerThreads = (1..Env.emptyConsumerTestThreads).map {
            thread {
                val consumer = DatabaseConsumer(Env.dataSource, queue)

                while (true) {
                    consumer.receive()
                    consumeCalls.incrementAndGet()
                }
            }
        }

        // Report stats
        thread {
            var lastCalls = 0L

            while (true) {
                TimeUnit.SECONDS.sleep(1)
                val current = consumeCalls.get()
                println("Consumer calls: ${current - lastCalls} calls/sec")
                lastCalls = current
            }
        }

        consumerThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue<ByteArray, Unit>(
            name = "empty_consumer_test",
            databaseDataType = PredefinedDataTypes.ByteArray,
            metadata = null
        )
    }
}
