package performance

object Env {

    val test = System.getenv("test")

    // ==========================================================================

    val pgHostname = System.getenv("host")

    val pgPort = System.getenv("port")?.toIntOrNull() ?: 5432

    val pgDatabase = System.getenv("database")

    val pgUser = System.getenv("user") ?: "postgres"

    val pgPassword = System.getenv("password") ?: ""

    // ==========================================================================

    val threads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

    val iterations = System.getenv("iterations")?.toIntOrNull() ?: 10_000_000

    val producerSendSize = System.getenv("send_size")?.toIntOrNull() ?: 1_000

    val producerBatchSize = System.getenv("batch_size")?.toIntOrNull() ?: 500

    val dataSize = System.getenv("data_size")?.toIntOrNull() ?: 500

    val dataSourceType = System.getenv("datasource") ?: "internal"

    val dataSource = if ("external" == dataSourceType) {
        PerformanceDataSourceProvider.externalDatasource()
    } else {
        PerformanceDataSourceProvider.internalDatasource()
    }

    // ==========================================================================

    fun report() {
        println("--------------------------------------------------")
        println("Test: $test")
        println("Threads: $threads")
        println("Iterations: $iterations")
        println("Producer send size: $producerSendSize")
        println("Producer batch size: $producerBatchSize")
        println("Data size: $dataSize")
        println("Data source: $dataSourceType")
        println("--------------------------------------------------")
    }
}
