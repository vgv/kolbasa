package kolbasa.queue

import kolbasa.cluster.ClusterStateUpdateConfig
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.mutator.Mutation
import kolbasa.mutator.MutationField
import kolbasa.schema.Const
import kolbasa.stats.prometheus.PrometheusConfig
import java.time.Duration

internal object Checks {

    fun checkDelay(delay: Duration?) {
        if (delay == null) return

        check(!delay.isNegative) {
            "delay must be greater than or equal to zero (current: $delay)"
        }
    }

    fun checkAttempts(attempts: Int?) {
        if (attempts == null) return

        check(attempts > 0) {
            "Attempts must be greater than zero (current: $attempts)"
        }
    }

    fun checkProducerName(producerName: String?) {
        if (producerName == null) return

        check(producerName.length < Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH) {
            "Producer name length must be less than ${Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH} symbols (current $producerName)"
        }

        // check all symbols
        check(producerName.all { it in Const.PRODUCER_CONSUMER_ALLOWED_SYMBOLS_SET }) {
            "Producer name contains illegal symbols. Allowed: ${Const.PRODUCER_CONSUMER_ALLOWED_SYMBOLS} (current=$producerName)"
        }
    }

    fun checkBatchSize(batchSize: Int) {
        check(batchSize >= 1) {
            "Batch size must be greater than or equal to 1 (current: $batchSize)"
        }
    }

    fun checkConsumerName(consumerName: String?) {
        if (consumerName == null) return

        check(consumerName.length < Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH) {
            "Consumer name length must be less than ${Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH} symbols (current=$consumerName)"
        }

        // check all symbols
        check(consumerName.all { it in Const.PRODUCER_CONSUMER_ALLOWED_SYMBOLS_SET }) {
            "Consumer name contains illegal symbols. Allowed: ${Const.PRODUCER_CONSUMER_ALLOWED_SYMBOLS} (current=$consumerName)"
        }
    }

    fun checkVisibilityTimeout(visibilityTimeout: Duration) {
        // We need some marker to check that visibilityTimeout is set by user
        // If it isn't set, we don't need to check it at all, because internal
        // default value has a special meaning and is always valid
        @Suppress("IDENTITY_SENSITIVE_OPERATIONS_WITH_VALUE_TYPE")
        if (visibilityTimeout === QueueOptions.VISIBILITY_TIMEOUT_NOT_SET) return

        check(!visibilityTimeout.isNegative) {
            "visibility timeout must be greater than or equal to zero (current: $visibilityTimeout)"
        }
    }

    fun checkQueueName(queueName: String) {
        check(queueName.isNotEmpty()) {
            "Queue name is empty"
        }

        check(!queueName.startsWith(Const.QUEUE_TABLE_NAME_PREFIX)) {
            "Queue name must not begin with '${Const.QUEUE_TABLE_NAME_PREFIX}'. Don't name your queue like 'q_customer_mail' - just 'customer_mail' is enough"
        }

        check(queueName.length <= Const.QUEUE_NAME_MAX_LENGTH) {
            "Queue name length must be less than or equal to ${Const.QUEUE_NAME_MAX_LENGTH} symbols (current=$queueName, length=${queueName.length})"
        }

        // check all symbols
        check(queueName.all { it in Const.QUEUE_NAME_ALLOWED_SYMBOLS_SET }) {
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
        check(fieldName.all { it in Const.META_FIELD_NAME_ALLOWED_SYMBOLS_SET }) {
            "Meta field name contains illegal symbols. Allowed: ${Const.META_FIELD_NAME_ALLOWED_SYMBOLS} (current=$fieldName)"
        }
    }

    fun checkSweepMaxMessages(messages: Int) {
        check(messages in SweepConfig.MIN_SWEEP_MESSAGES..SweepConfig.MAX_SWEEP_MESSAGES) {
            "Sweep max messages must be in the [${SweepConfig.MIN_SWEEP_MESSAGES}..${SweepConfig.MAX_SWEEP_MESSAGES}] range"
        }
    }

    fun checkSweepMaxIterations(maxIterations: Int) {
        check(maxIterations in SweepConfig.MIN_SWEEP_ITERATIONS..SweepConfig.MAX_SWEEP_ITERATIONS) {
            "Sweep max iterations must be in the [${SweepConfig.MIN_SWEEP_ITERATIONS}..${SweepConfig.MAX_SWEEP_ITERATIONS}] range"
        }
    }

    fun checkSweepPeriod(period: Int) {
        check(period in SweepConfig.MIN_SWEEP_PERIOD..SweepConfig.MAX_SWEEP_PERIOD) {
            "Sweep period must be in the [${SweepConfig.MIN_SWEEP_PERIOD}..${SweepConfig.MAX_SWEEP_PERIOD}] range"
        }
    }

    fun checkCustomQueueSizeMeasureInterval(queueName: String, customDuration: Duration) {
        check(customDuration >= PrometheusConfig.Config.MIN_QUEUE_SIZE_MEASURE_INTERVAL) {
            "Custom queue size measure interval must be greater than or equal to ${PrometheusConfig.Config.MIN_QUEUE_SIZE_MEASURE_INTERVAL} (current: $customDuration, queue=$queueName)"
        }
    }

    fun checkClusterStateUpdateInterval(interval: Duration) {
        check(interval >= ClusterStateUpdateConfig.MIN_INTERVAL) {
            "Cluster state update interval must be greater than or equal to ${ClusterStateUpdateConfig.MIN_INTERVAL} (current: $interval)"
        }
    }

    fun checkMutations(mutations: List<Mutation>) {
        val mutationsByField = mutations.groupBy { mutation ->
            when (mutation) {
                is MutationField.RemainingAttemptField -> MutationField.RemainingAttemptField::class
                is MutationField.ScheduledAtField -> MutationField.ScheduledAtField::class
                else -> throw IllegalStateException("Mutation $mutation doesn't implement ${MutationField::class}")
            }
        }

        mutationsByField.forEach { (_, fieldMutations) ->
            check(fieldMutations.size == 1) {
                "Can't mutate one field more than once: $fieldMutations"
            }
        }
    }

}
