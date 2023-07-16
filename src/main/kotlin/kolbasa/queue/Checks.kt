package kolbasa.queue

import kolbasa.schema.Const
import java.time.Duration

internal object Checks {

    fun checkDelay(delay: Duration) {
        if (delay === QueueOptions.DELAY_NOT_SET) return

        check(!delay.isNegative) {
            "delay must be greater than or equal to zero (current: $delay)"
        }
    }

    fun checkAttempts(attempts: Int) {
        if (attempts == QueueOptions.ATTEMPTS_NOT_SET) return

        check(attempts > 0) {
            "Attempts must be greater than zero (current: $attempts)"
        }
    }

    fun checkProducerName(producer: String?) {
        if (producer == null) return

        check(producer.length < Const.PRODUCER_CONSUMER_VALUE_LENGTH) {
            "Producer name length must be less than ${Const.PRODUCER_CONSUMER_VALUE_LENGTH} symbols (current $producer)"
        }
    }

    fun checkProducerBatchSize(batchSize: Int) {
        check(batchSize >= 1) {
            "Producer options batch size must be greater than or equal to 1 (current: $batchSize)"
        }
    }


    fun checkConsumerName(consumer: String?) {
        if (consumer == null) return

        check(consumer.length < Const.PRODUCER_CONSUMER_VALUE_LENGTH) {
            "Consumer name length must be less than ${Const.PRODUCER_CONSUMER_VALUE_LENGTH} symbols (current=$consumer)"
        }
    }

    fun checkVisibilityTimeout(visibilityTimeout: Duration) {
        if (visibilityTimeout === QueueOptions.VISIBILITY_TIMEOUT_NOT_SET) return

        check(!visibilityTimeout.isNegative) {
            "visibility timeout must be greater than or equal to zero (current: $visibilityTimeout)"
        }
    }

    fun checkQueueName(queueName: String) {
        check(queueName.isNotEmpty()) {
            "Queue name is empty"
        }

        check(queueName.length <= Const.QUEUE_NAME_MAX_LENGTH) {
            "Queue name length must be less than or equal to ${Const.QUEUE_NAME_MAX_LENGTH} symbols (current=$queueName, length=${queueName.length})"
        }

        // check all symbols
        val allowedSymbols = Const.QUEUE_NAME_ALLOWED_SYMBOLS.toSet()
        check(queueName.all { it in allowedSymbols }) {
            "Queue name contains illegal symbols. Allowed: ${Const.QUEUE_NAME_ALLOWED_SYMBOLS} (current=$queueName)"
        }
    }

    fun checkMetaFieldName(fieldName: String) {
        check(fieldName.isNotEmpty()) {
            "Meta field name is empty"
        }

        check(fieldName.length <= Const.META_FIELD_NAME_MAX_LENGTH) {
            "Meta field name length must be less than or equal to ${Const.META_FIELD_NAME_MAX_LENGTH} symbols (current=$fieldName, length=${fieldName.length})"
        }

        // check all symbols
        val allowedSymbols = Const.META_FIELD_NAME_ALLOWED_SYMBOLS.toSet()
        check(fieldName.all { it in allowedSymbols }) {
            "Meta field name contains illegal symbols. Allowed: ${Const.META_FIELD_NAME_ALLOWED_SYMBOLS} (current=$fieldName)"
        }
    }

    fun checkCleanupLimit(limit: Int) {
        check(limit in Const.MIN_CLEANUP_ROWS..Const.MAX_CLEANUP_ROWS) {
            "Cleanup limit must be in the [${Const.MIN_CLEANUP_ROWS}..${Const.MAX_CLEANUP_ROWS}] range"
        }
    }

    fun checkCleanupMaxIterations(maxIterations: Int) {
        check(maxIterations in Const.MIN_CLEANUP_ITERATIONS..Const.MAX_CLEANUP_ITERATIONS) {
            "Cleanup max iterations must be in the [${Const.MIN_CLEANUP_ITERATIONS}..${Const.MAX_CLEANUP_ITERATIONS}] range"
        }
    }

    fun checkCleanupProbabilityPercent(probabilityPercent: Int) {
        check(probabilityPercent in Const.MIN_CLEANUP_PROBABILITY_PERCENT..Const.MAX_CLEANUP_PROBABILITY_PERCENT) {
            "Cleanup probability percent must be in the [${Const.MIN_CLEANUP_PROBABILITY_PERCENT}..${Const.MAX_CLEANUP_PROBABILITY_PERCENT}] range"
        }
    }

}
