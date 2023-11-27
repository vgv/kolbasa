package performance

object Env {

    val test = System.getenv("test")

    // ==========================================================================

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

    // ==========================================================================

    val pThreads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

    val pSendSize = System.getenv("send_size")?.toIntOrNull() ?: 1_000

    val pBatchSize = System.getenv("batch_size")?.toIntOrNull() ?: 500

    val pDataSizeBytes = System.getenv("data_size_bytes")?.toIntOrNull() ?: 500

    // ==========================================================================

    val ecThreads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

    // ==========================================================================

    val pcProducerThreads: Int = System.getenv("producer-threads")?.toIntOrNull() ?: 1

    val pcConsumerThreads: Int = System.getenv("consumer-threads")?.toIntOrNull() ?: 1

    val pcQueueSizeBaseline = System.getenv("queue_size_baseline")?.toIntOrNull() ?: 0

    val pcSendSize = System.getenv("send_size")?.toIntOrNull() ?: 1_000

    val pcBatchSize = System.getenv("batch_size")?.toIntOrNull() ?: 500

    val pcDataSizeBytes = System.getenv("data_size_bytes")?.toIntOrNull() ?: 500

    val pcConsumerReceiveLimit = System.getenv("consumer_receive_limit")?.toIntOrNull() ?: 1000

    // ==========================================================================

    private fun generalReport() {
        println("Test: $test")
        println("Data source type: $dataSourceType")
        if ("external" == dataSourceType) {
            println("PG host: $pgHostname")
            println("PG port: $pgPort")
            println("PG database: $pgDatabase")
            println("PG user: $pgUser")
        }
    }

    fun reportProducerTestEnv() {
        println("--------------------------------------------------")
        generalReport()
        println("Threads: $pThreads")
        println("Producer send size: $pSendSize")
        println("Producer batch size: $pBatchSize")
        println("Data size: $pDataSizeBytes")
        println("--------------------------------------------------")
    }

    fun reportEmptyConsumerTestEnv() {
        println("--------------------------------------------------")
        generalReport()
        println("Threads: $ecThreads")
        println("--------------------------------------------------")
    }

    fun reportProducerConsumerTestEnv() {
        println("--------------------------------------------------")
        generalReport()
        println("Producer threads: $pcProducerThreads")
        println("Consumer threads: $pcConsumerThreads")
        println("Queue size baseline: $pcQueueSizeBaseline")
        println("Producer send size: $pcSendSize")
        println("Producer batch size: $pcBatchSize")
        println("Data size: $pcDataSizeBytes")
        println("Consumer receive limit: $pcConsumerReceiveLimit")
        println("--------------------------------------------------")
    }

}
