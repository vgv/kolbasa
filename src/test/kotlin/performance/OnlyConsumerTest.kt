package performance

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class OnlyConsumerTest : PerformanceTest {

    override fun run() {
        Env.reportOnlyConsumerTestEnv()

        // Update
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        // Truncate table before test
        Env.Common.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        val consumeCalls = AtomicLong()

        val consumer = DatabaseConsumer(Env.Common.dataSource)

        val consumerThreads = (1..Env.OnlyConsumer.threads).map {
            thread {
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

                val seconds = ((System.currentTimeMillis() - start) / 1000)
                val currentCalls = consumeCalls.get() / seconds

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
    OnlyConsumerTest().run()
}
