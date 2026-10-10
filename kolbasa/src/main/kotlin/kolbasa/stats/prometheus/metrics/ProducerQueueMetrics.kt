package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.Extensions.incInt
import kolbasa.stats.prometheus.metrics.Extensions.incLong
import kolbasa.stats.prometheus.metrics.Extensions.observeNanos

internal class ProducerQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    prometheusConfig: PrometheusConfig.Config
) {

    fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long
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
}
