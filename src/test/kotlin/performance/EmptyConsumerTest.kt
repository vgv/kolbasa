package performance

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class EmptyConsumerTest : PerformanceTest {

    override fun run() {
        Env.reportEmptyConsumerTestEnv()

        // Update
        SchemaHelpers.createOrUpdateQueues(Env.dataSource, queue)

        // Truncate table before test
        Env.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        val consumeCalls = AtomicLong()

        val consumerThreads = (1..Env.ecThreads).map {
            thread {
                val consumer = DatabaseConsumer(Env.dataSource)

                while (true) {
                    consumer.receive(queue)
                    consumeCalls.incrementAndGet()
                }
            }
        }

        // Report stats
        thread {
            val start = System.currentTimeMillis()

            while (true) {
                TimeUnit.SECONDS.sleep(1)
                val currentCalls = consumeCalls.get() / ((System.currentTimeMillis() - start) / 1000)
                val seconds = ((System.currentTimeMillis() - start) / 1000)
                println("Seconds: $seconds, consumer calls: $currentCalls calls/sec")
                println("-------------------------------------------")
            }
        }

        consumerThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue.of(
            name = "empty_consumer_test",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    EmptyConsumerTest().run()
}
