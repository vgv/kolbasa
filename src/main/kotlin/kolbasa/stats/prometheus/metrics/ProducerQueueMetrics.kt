package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import io.prometheus.metrics.core.datapoints.GaugeDataPoint
import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.Extensions.incInt
import kolbasa.stats.prometheus.metrics.Extensions.incLong
import kolbasa.stats.prometheus.metrics.Extensions.observeNanos
import kolbasa.stats.prometheus.queuesize.QueueSizeCache
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

internal class PrometheusQueueMetrics(
    private val queueName: String,
    private val prometheusConfig: PrometheusConfig.Config
) : QueueMetrics {

    private val producer: ConcurrentMap<NodeId, ProducerQueueMetrics> = ConcurrentHashMap()
    private val consumer: ConcurrentMap<NodeId, ConsumerQueueMetrics> = ConcurrentHashMap()
    private val sweep: ConcurrentMap<NodeId, SweepQueueMetrics> = ConcurrentHashMap()
    private val mutator: ConcurrentMap<NodeId, MutatorQueueMetrics> = ConcurrentHashMap()

    override fun usePreciseStringSize(): Boolean {
        return prometheusConfig.preciseStringSize
    }

    override fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
        val producerQueueMetrics = producer.computeIfAbsent(nodeId) { _ ->
            ProducerQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        producerQueueMetrics.producerSendMetrics(
            partialInsert = partialInsert,
            allMessages = allMessages,
            failedMessages = failedMessages,
            executionNanos = executionNanos,
            approxBytes = approxBytes,
            queueSizeCalcFunc = queueSizeCalcFunc
        )
    }

    override fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
        val consumerQueueMetrics = consumer.computeIfAbsent(nodeId) { _ ->
            ConsumerQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        consumerQueueMetrics.consumerReceiveMetrics(
            receivedMessages = receivedMessages,
            executionNanos = executionNanos,
            approxBytes = approxBytes,
            queueSizeCalcFunc = queueSizeCalcFunc
        )
    }

    override fun consumerDeleteMetrics(
        nodeId: NodeId,
        removedMessages: Int,
        executionNanos: Long
    ) {
        val consumerQueueMetrics = consumer.computeIfAbsent(nodeId) { _ ->
            ConsumerQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        consumerQueueMetrics.consumerDeleteMetrics(
            removedMessages = removedMessages,
            executionNanos = executionNanos
        )
    }

    override fun sweepMetrics(
        nodeId: NodeId,
        iterations: Int,
        removedMessages: Int,
        executionNanos: Long
    ) {
        val sweepQueueMetrics = sweep.computeIfAbsent(nodeId) { _ ->
            SweepQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        sweepQueueMetrics.sweepMetrics(
            iterations = iterations,
            removedMessages = removedMessages,
            executionNanos = executionNanos
        )
    }

    override fun mutatorMetrics(
        nodeId: NodeId,
        iterations: Int,
        mutatedMessages: Int,
        executionNanos: Long,
        byId: Boolean
    ) {
        val mutatorQueueMetrics = mutator.computeIfAbsent(nodeId) { _ ->
            MutatorQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        mutatorQueueMetrics.mutatorMetrics(
            iterations = iterations,
            mutatedMessages = mutatedMessages,
            executionNanos = executionNanos,
            byId = byId
        )
    }
}

internal class ProducerQueueMetrics(
    private val queueName: String,
    private val nodeId: NodeId,
    private val prometheusConfig: PrometheusConfig.Config
) {

    private val queueSizeCacheKey = "${queueName}-${nodeId.id}"

    fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
        when (partialInsert) {
            PartialInsert.PROHIBITED -> {
                producerSendCounterProhibited.inc()
                producerSendMessagesCounterProhibited.incInt(allMessages)
                producerSendFailedMessagesCounterProhibited.incInt(failedMessages)
                producerSendDurationProhibited.observeNanos(executionNanos)
                producerSendBytesCounterProhibited.incLong(approxBytes)
            }

            PartialInsert.UNTIL_FIRST_FAILURE -> {
                producerSendCounterUntilFirstFailure.inc()
                producerSendMessagesCounterUntilFirstFailure.incInt(allMessages)
                producerSendFailedMessagesCounterUntilFirstFailure.incInt(failedMessages)
                producerSendDurationUntilFirstFailure.observeNanos(executionNanos)
                producerSendBytesCounterUntilFirstFailure.incLong(approxBytes)
            }

            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> {
                producerSendCounterInsertAsManyAsPossible.inc()
                producerSendMessagesCounterInsertAsManyAsPossible.incInt(allMessages)
                producerSendFailedMessagesCounterInsertAsManyAsPossible.incInt(failedMessages)
                producerSendDurationInsertAsManyAsPossible.observeNanos(executionNanos)
                producerSendBytesCounterInsertAsManyAsPossible.incLong(approxBytes)
            }
        }

        // Queue size
        // We use internal caching to prevent too frequent calls to the database
        val queueSizeMeasureInterval = prometheusConfig.customQueueSizeMeasureInterval[queueName]
            ?: PrometheusConfig.Config.DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL
        val queueSize = QueueSizeCache.get(queueSizeCacheKey, queueSizeMeasureInterval, queueSizeCalcFunc)
        producerQueueSizeGauge.set(queueSize.toDouble())
    }

    private val producerSendCounterProhibited: CounterDataPoint = prometheusConfig.producerSendCounter
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendCounterUntilFirstFailure: CounterDataPoint = prometheusConfig.producerSendCounter
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendCounterInsertAsManyAsPossible: CounterDataPoint = prometheusConfig.producerSendCounter
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendMessagesCounterProhibited: CounterDataPoint = prometheusConfig.producerSendMessagesCounter
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendMessagesCounterUntilFirstFailure: CounterDataPoint = prometheusConfig.producerSendMessagesCounter
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendMessagesCounterInsertAsManyAsPossible: CounterDataPoint = prometheusConfig.producerSendMessagesCounter
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendFailedMessagesCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendFailedMessagesCounter
            .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendFailedMessagesCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendFailedMessagesCounter
            .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendFailedMessagesCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendFailedMessagesCounter
            .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendDurationProhibited: DistributionDataPoint = prometheusConfig.producerSendDuration
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendDurationUntilFirstFailure: DistributionDataPoint = prometheusConfig.producerSendDuration
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendDurationInsertAsManyAsPossible: DistributionDataPoint = prometheusConfig.producerSendDuration
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendBytesCounterProhibited: CounterDataPoint = prometheusConfig.producerSendBytesCounter
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendBytesCounterUntilFirstFailure: CounterDataPoint = prometheusConfig.producerSendBytesCounter
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendBytesCounterInsertAsManyAsPossible: CounterDataPoint = prometheusConfig.producerSendBytesCounter
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerQueueSizeGauge: GaugeDataPoint = prometheusConfig.producerQueueSizeGauge
        .labelValues(queueName, nodeId.id)
}

internal class ConsumerQueueMetrics(
    private val queueName: String,
    private val nodeId: NodeId,
    private val prometheusConfig: PrometheusConfig.Config
) {

    private val queueSizeCacheKey = "${queueName}-${nodeId.id}"

    fun consumerReceiveMetrics(
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
        consumerReceiveCounter.inc()
        consumerReceiveBytesCounter.incLong(approxBytes)
        consumerReceiveMessagesCounter.incInt(receivedMessages)
        consumerReceiveDuration.observeNanos(executionNanos)

        // Queue size
        // We use internal caching to prevent too frequent calls to the database
        val queueSizeMeasureInterval = prometheusConfig.customQueueSizeMeasureInterval[queueName]
            ?: PrometheusConfig.Config.DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL
        val queueSize = QueueSizeCache.get(queueSizeCacheKey, queueSizeMeasureInterval, queueSizeCalcFunc)
        consumerQueueSizeGauge.set(queueSize.toDouble())
    }

    fun consumerDeleteMetrics(removedMessages: Int, executionNanos: Long) {
        consumerDeleteCounter.inc()
        consumerDeleteMessagesCounter.incInt(removedMessages)
        consumerDeleteDuration.observeNanos(executionNanos)
    }

    private val consumerReceiveCounter: CounterDataPoint = prometheusConfig.consumerReceiveCounter
        .labelValues(queueName, nodeId.id)
    private val consumerReceiveBytesCounter: CounterDataPoint = prometheusConfig.consumerReceiveBytesCounter
        .labelValues(queueName, nodeId.id)
    private val consumerReceiveMessagesCounter: CounterDataPoint = prometheusConfig.consumerReceiveMessagesCounter
        .labelValues(queueName, nodeId.id)
    private val consumerReceiveDuration: DistributionDataPoint = prometheusConfig.consumerReceiveDuration
        .labelValues(queueName, nodeId.id)

    private val consumerDeleteCounter: CounterDataPoint = prometheusConfig.consumerDeleteCounter
        .labelValues(queueName, nodeId.id)
    private val consumerDeleteMessagesCounter: CounterDataPoint = prometheusConfig.consumerDeleteMessagesCounter
        .labelValues(queueName, nodeId.id)
    private val consumerDeleteDuration: DistributionDataPoint = prometheusConfig.consumerDeleteDuration
        .labelValues(queueName, nodeId.id)
    private val consumerQueueSizeGauge: GaugeDataPoint = prometheusConfig.consumerQueueSizeGauge
        .labelValues(queueName, nodeId.id)
}

internal class SweepQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    prometheusConfig: PrometheusConfig.Config
) {

    fun sweepMetrics(iterations: Int, removedMessages: Int, executionNanos: Long) {
        sweepCounter.inc()
        sweepIterationsCounter.incInt(iterations)
        sweepMessagesRemovedCounter.incInt(removedMessages)
        sweepDuration.observeNanos(executionNanos)
    }

    private val sweepCounter: CounterDataPoint = prometheusConfig.sweepCounter
        .labelValues(queueName, nodeId.id)
    private val sweepIterationsCounter: CounterDataPoint = prometheusConfig.sweepIterationsCounter
        .labelValues(queueName, nodeId.id)
    private val sweepMessagesRemovedCounter: CounterDataPoint = prometheusConfig.sweepMessagesRemovedCounter
        .labelValues(queueName, nodeId.id)
    private val sweepDuration: DistributionDataPoint = prometheusConfig.sweepDuration
        .labelValues(queueName, nodeId.id)
}

internal class MutatorQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    prometheusConfig: PrometheusConfig.Config
) {

    fun mutatorMetrics(iterations: Int, mutatedMessages: Int, executionNanos: Long, byId: Boolean) {
        if (byId) {
            mutatorMutateCounterById.inc()
            mutatorMutateIterationsCounterById.incInt(iterations)
            mutatorMutateMessagesCounterById.incInt(mutatedMessages)
            mutatorMutateDurationById.observeNanos(executionNanos)
        } else {
            mutatorMutateCounterByFilter.inc()
            mutatorMutateIterationsCounterByFilter.incInt(iterations)
            mutatorMutateMessagesCounterByFilter.incInt(mutatedMessages)
            mutatorMutateDurationByFilter.observeNanos(executionNanos)
        }
    }

    private val mutatorMutateCounterById: CounterDataPoint = prometheusConfig.mutatorMutateCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateIterationsCounterById: CounterDataPoint = prometheusConfig.mutatorMutateIterationsCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateMessagesCounterById: CounterDataPoint = prometheusConfig.mutatorMutateMessagesCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateDurationById: DistributionDataPoint = prometheusConfig.mutatorMutateDuration
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)

    private val mutatorMutateCounterByFilter: CounterDataPoint = prometheusConfig.mutatorMutateCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateIterationsCounterByFilter: CounterDataPoint = prometheusConfig.mutatorMutateIterationsCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateMessagesCounterByFilter: CounterDataPoint = prometheusConfig.mutatorMutateMessagesCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateDurationByFilter: DistributionDataPoint = prometheusConfig.mutatorMutateDuration
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)

    private companion object {
        const val MUTATOR_TYPE_BY_ID = "by_id"
        const val MUTATOR_TYPE_BY_FILTER = "by_filter"
    }
}
