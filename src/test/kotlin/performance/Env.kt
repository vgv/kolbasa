package performance

import java.time.Duration

object Env {

    val test = System.getenv("test")

    // ==========================================================================

    object Common {
        val pgHostname = System.getenv("host")

        val pgPort = System.getenv("port")?.toIntOrNull() ?: 5432

        val pgDatabase = System.getenv("database")

        val pgUser = System.getenv("user") ?: "postgres"

        val pgPassword = System.getenv("password") ?: ""

        val dataSourceType = System.getenv("datasource") ?: "internal"

        val dataSource = if ("external" == dataSourceType) {
            PerformanceDataSourceProvider.externalDatasource()
        } else {
            PerformanceDataSourceProvider.internalDatasource()
        }

        //
        val pauseBeforeStart = Duration.ofMillis(System.getenv("pause-millis")?.toLongOrNull() ?: 0)
    }


    // ==========================================================================

    object OnlyProducer {
        val threads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

        val sendSize = System.getenv("send-size")?.toIntOrNull() ?: 1_000

        val batchSize = System.getenv("batch-size")?.toIntOrNull() ?: 500

        val dataSizeBytes = System.getenv("data-size-bytes")?.toIntOrNull() ?: 500
    }

    // ==========================================================================

    object OnlyConsumer {
        val threads: Int = System.getenv("threads")?.toIntOrNull() ?: 1
    }

    // ==========================================================================

    object ProducerConsumer {
        val producerThreads: Int = System.getenv("producer-threads")?.toIntOrNull() ?: 1

        val consumerThreads: Int = System.getenv("consumer-threads")?.toIntOrNull() ?: 1

        val queueSizeBaseline = System.getenv("queue-size-baseline")?.toIntOrNull() ?: 0

        val sendSize = System.getenv("send-size")?.toIntOrNull() ?: 1_000

        val batchSize = System.getenv("batch-size")?.toIntOrNull() ?: 500

        val dataSizeBytes = System.getenv("data-size-bytes")?.toIntOrNull() ?: 500

        val consumerReceiveLimit = System.getenv("consumer-receive-limit")?.toIntOrNull() ?: 1000
    }

    // ==========================================================================

    private fun commonReport() {
        println("Test: $test")
        println("Pause before start: ${Common.pauseBeforeStart.toMillis()} ms")
        println("Data source type: ${Common.dataSourceType}")
        if ("external" == Common.dataSourceType) {
            println("PG host: ${Common.pgHostname}")
            println("PG port: ${Common.pgPort}")
            println("PG database: ${Common.pgDatabase}")
            println("PG user: ${Common.pgUser}")
        }
    }

    fun reportOnlyProducerTestEnv() {
        println("--------------------------------------------------")
        commonReport()
        println("Threads: ${OnlyProducer.threads}")
        println("Producer send size: ${OnlyProducer.sendSize}")
        println("Producer batch size: ${OnlyProducer.batchSize}")
        println("Data size: ${OnlyProducer.dataSizeBytes}")
        println("--------------------------------------------------")
    }

    fun reportOnlyConsumerTestEnv() {
        println("--------------------------------------------------")
        commonReport()
        println("Threads: ${OnlyConsumer.threads}")
        println("--------------------------------------------------")
    }

    fun reportProducerConsumerTestEnv() {
        println("--------------------------------------------------")
        commonReport()
        println("Producer threads: ${ProducerConsumer.producerThreads}")
        println("Consumer threads: ${ProducerConsumer.consumerThreads}")
        println("Queue size baseline: ${ProducerConsumer.queueSizeBaseline}")
        println("Producer send size: ${ProducerConsumer.sendSize}")
        println("Producer batch size: ${ProducerConsumer.batchSize}")
        println("Data size: ${ProducerConsumer.dataSizeBytes}")
        println("Consumer receive limit: ${ProducerConsumer.consumerReceiveLimit}")
        println("--------------------------------------------------")
    }
}
