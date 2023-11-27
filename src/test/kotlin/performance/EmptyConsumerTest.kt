package performance

import kolbasa.consumer.DatabaseConsumer
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
        SchemaHelpers.updateDatabaseSchema(Env.dataSource, queue)

        // Truncate table before test
        Env.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        val consumeCalls = AtomicLong()

        val consumerThreads = (1..Env.ecThreads).map {
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
            val start = System.currentTimeMillis()

            while (true) {
                TimeUnit.SECONDS.sleep(1)
                val currentCalls = consumeCalls.get() / ((System.currentTimeMillis() - start) / 1000)
                println("Consumer calls: $currentCalls calls/sec")
                println("-------------------------------------------")
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

fun main() {
    EmptyConsumerTest().run()
}
