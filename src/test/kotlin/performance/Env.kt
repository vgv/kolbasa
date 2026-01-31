package performance

import java.time.Duration

object Env {

    val test: String = System.getenv("test")

    // ==========================================================================

    object Common {
        val pgHostname: String? = System.getenv("host")

        val pgPort = System.getenv("port")?.toIntOrNull() ?: 5432

        val pgDatabase: String? = System.getenv("database")

        val pgUser = System.getenv("user") ?: "postgres"

        val pgPassword = System.getenv("password") ?: ""

        val dataSourceType = System.getenv("datasource") ?: "internal"

        val dataSource = if ("external" == dataSourceType) {
            PerformanceDataSourceProvider.externalDatasource()
        } else {
            PerformanceDataSourceProvider.internalDatasource()
        }

        val pauseBeforeStart: Duration = Duration.ofSeconds(System.getenv("pause-before")?.toLongOrNull() ?: 0)
        val pauseAfterFinish: Duration = Duration.ofSeconds(System.getenv("pause-after")?.toLongOrNull() ?: 0)

        fun report() {
            println("Test: $test")
            println("Pause before start: ${pauseBeforeStart.toSeconds()} seconds")
            println("Pause after finish: ${pauseAfterFinish.toSeconds()} seconds")
            println("Data source type: $dataSourceType")
            if ("external" == dataSourceType) {
                println("PG host: $pgHostname")
                println("PG port: $pgPort")
                println("PG database: $pgDatabase")
                println("PG user: $pgUser")
            }
        }
    }


    // ==========================================================================

    object OnlySend {
        const val TEST_NAME = "only-send"

        val threads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

        val totalSendCalls = System.getenv("total-send-calls")?.toLongOrNull() ?: Long.MAX_VALUE
        val oneSendMessages = System.getenv("one-send-messages")?.toIntOrNull() ?: 1_000
        val batchSize = System.getenv("batch-size")?.toIntOrNull() ?: 500
        val oneMessageSizeBytes = System.getenv("one-message-size-bytes")?.toIntOrNull() ?: 500

        fun report() {
            println("--------------------------------------------------")
            Common.report()
            println("Threads: $threads")
            println("Total send calls: ${if (totalSendCalls == Long.MAX_VALUE) "unlimited" else totalSendCalls}")
            println("Messages per one send call: $oneSendMessages")
            println("Producer batch size: $batchSize")
            println("One message size (bytes): $oneMessageSizeBytes")
            println("--------------------------------------------------")
        }
    }

    object OnlyReceive {
        const val TEST_NAME = "only-receive"

        val threads: Int = System.getenv("threads")?.toIntOrNull() ?: 1
        val messages = System.getenv("messages")?.toIntOrNull() ?: 1_000_000
        val oneMessageSizeBytes = System.getenv("one-message-size-bytes")?.toIntOrNull() ?: 500
        val consumerReceiveLimit = System.getenv("consumer-receive-limit")?.toIntOrNull() ?: 1000

        fun report() {
            println("--------------------------------------------------")
            Common.report()
            println("Consumer threads: $threads")
            println("Messages: $messages")
            println("One message size (bytes): $oneMessageSizeBytes")
            println("Consumer receive limit: $consumerReceiveLimit")
            println("--------------------------------------------------")
        }
    }

    // ==========================================================================

    object EmptyReceive {
        const val TEST_NAME = "empty-receive"

        val threads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

        val totalReceiveCalls = System.getenv("total-receive-calls")?.toLongOrNull() ?: Long.MAX_VALUE

        fun report() {
            println("--------------------------------------------------")
            Common.report()
            println("Threads: $threads")
            println("Total receive calls: ${if (totalReceiveCalls == Long.MAX_VALUE) "unlimited" else totalReceiveCalls}")
            println("--------------------------------------------------")
        }
    }

    object EmptyDelete {
        const val TEST_NAME = "empty-delete"

        val threads: Int = System.getenv("threads")?.toIntOrNull() ?: 1

        val totalDeleteCalls = System.getenv("total-delete-calls")?.toLongOrNull() ?: Long.MAX_VALUE
        val oneDeleteMessages = System.getenv("one-delete-messages")?.toIntOrNull() ?: 100

        fun report() {
            println("--------------------------------------------------")
            Common.report()
            println("Threads: $threads")
            println("Total delete calls: ${if (totalDeleteCalls == Long.MAX_VALUE) "unlimited" else totalDeleteCalls}")
            println("Messages per one delete call: $oneDeleteMessages")
            println("--------------------------------------------------")
        }
    }

    // ==========================================================================


    object ProducerConsumer {
        const val TEST_NAME = "producer-consumer"

        val producerThreads: Int = System.getenv("producer-threads")?.toIntOrNull() ?: 1

        val consumerThreads: Int = System.getenv("consumer-threads")?.toIntOrNull() ?: 1

        val totalSendCalls = System.getenv("total-send-calls")?.toLongOrNull() ?: Long.MAX_VALUE

        val queueSizeBaseline = System.getenv("queue-size-baseline")?.toIntOrNull() ?: 0

        val oneSendMessages = System.getenv("one-send-messages")?.toIntOrNull() ?: 1_000

        val batchSize = System.getenv("batch-size")?.toIntOrNull() ?: 500

        val oneMessageSizeBytes = System.getenv("one-message-size-bytes")?.toIntOrNull() ?: 500

        val consumerReceiveLimit = System.getenv("consumer-receive-limit")?.toIntOrNull() ?: 1000

        fun report() {
            println("--------------------------------------------------")
            Common.report()
            println("Producer threads: $producerThreads")
            println("Consumer threads: $consumerThreads")
            println("Total send calls: ${if (totalSendCalls == Long.MAX_VALUE) "unlimited" else totalSendCalls}")
            println("Queue size baseline: $queueSizeBaseline")
            println("Messages per one send call: $oneSendMessages")
            println("Producer batch size: $batchSize")
            println("One message size (bytes): $oneMessageSizeBytes")
            println("Consumer receive limit: $consumerReceiveLimit")
            println("--------------------------------------------------")
        }
    }
}
