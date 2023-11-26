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

    val producerTestThreads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

    val producerTestIterations = System.getenv("iterations")?.toIntOrNull() ?: 10_000_000

    val producerTestSendSize = System.getenv("send_size")?.toIntOrNull() ?: 1_000

    val producerTestBatchSize = System.getenv("batch_size")?.toIntOrNull() ?: 500

    val producerTestDataSizeBytes = System.getenv("data_size")?.toIntOrNull() ?: 500

    // ==========================================================================

    val emptyConsumerTestThreads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

    // ==========================================================================

    private fun generalReport() {
        println("Test: $test")
        println("Data source type: $dataSourceType")
        if ("external" == dataSourceType) {
            println("PG host: $pgHostname")
            println("PG port: $pgPort")
            println("PG database: $pgDatabase")
            println("PG user: $pgUser")
            println("PG password: ${pgPassword.length} symbols")
        }
    }

    fun reportProducerTestEnv() {
        println("--------------------------------------------------")
        generalReport()
        println("Threads: $producerTestThreads")
        println("Iterations: $producerTestIterations")
        println("Producer send size: $producerTestSendSize")
        println("Producer batch size: $producerTestBatchSize")
        println("Data size: $producerTestDataSizeBytes")
        println("--------------------------------------------------")
    }

    fun reportEmptyConsumerTestEnv() {
        println("--------------------------------------------------")
        generalReport()
        println("Threads: $emptyConsumerTestThreads")
        println("--------------------------------------------------")
    }
}
