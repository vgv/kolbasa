package performance

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.Id
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.random.Random

class EmptyDeleteTest : PerformanceTest {

    override fun run() {
        Env.EmptyDelete.report()

        // Update
        SchemaHelpers.createOrUpdateQueues(Env.Common.dataSource, queue)

        val randomIdsToDelete = (1..1000).map {
            (1..Env.EmptyDelete.oneDeleteMessages).map {
                Id(Random.nextLong(), Random.nextInt())
            }
        }

        // Truncate table before test
        Env.Common.dataSource.useStatement { statement ->
            statement.execute("TRUNCATE TABLE ${queue.dbTableName}")
        }

        val deleteCalls = AtomicLong()

        val consumer = DatabaseConsumer(Env.Common.dataSource)

        val deleteThreads = (1..Env.EmptyDelete.threads).map {
            thread {
                while (deleteCalls.incrementAndGet() <= Env.EmptyDelete.totalDeleteCalls) {
                    consumer.delete(queue, randomIdsToDelete.random())
                }
            }
        }

        // Report stats
        thread {
            val start = System.currentTimeMillis()

            while (deleteCalls.get() < Env.EmptyDelete.totalDeleteCalls) {
                TimeUnit.SECONDS.sleep(1)

                val seconds = ((System.currentTimeMillis() - start) / 1000)

                val delCalls = deleteCalls.get()
                val deleteRate = delCalls / seconds

                println("Time: $seconds s, delete calls: $delCalls ($deleteRate calls/s)")
                println("-------------------------------------------")
            }
        }

        deleteThreads.forEach { it.join() }
    }

    companion object {
        private val queue = Queue.of(
            name = "empty_delete_test_queue",
            databaseDataType = PredefinedDataTypes.ByteArray
        )
    }
}

fun main() {
    EmptyDeleteTest().run()
}
