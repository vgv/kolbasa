package kolbasa.telemetry.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import kolbasa.telemetry.prometheus.metrics.Extensions.incInt
import kolbasa.telemetry.prometheus.metrics.Extensions.incLong
import kolbasa.telemetry.prometheus.metrics.Extensions.observeNanos

internal class ProducerQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: PrometheusMetrics
) {

    fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        bytesSent: Long
    ) {
        when (partialInsert) {
            PartialInsert.PROHIBITED -> {
                producerSendCounterProhibited.inc()
                producerSendMessagesCounterProhibited.incInt(allMessages)
                producerSendFailedMessagesCounterProhibited.incInt(failedMessages)
                producerSendDurationProhibited.observeNanos(executionNanos)
                producerSendBytesCounterProhibited.incLong(bytesSent)
            }

            PartialInsert.UNTIL_FIRST_FAILURE -> {
                producerSendCounterUntilFirstFailure.inc()
                producerSendMessagesCounterUntilFirstFailure.incInt(allMessages)
                producerSendFailedMessagesCounterUntilFirstFailure.incInt(failedMessages)
                producerSendDurationUntilFirstFailure.observeNanos(executionNanos)
                producerSendBytesCounterUntilFirstFailure.incLong(bytesSent)
            }

            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> {
                producerSendCounterInsertAsManyAsPossible.inc()
                producerSendMessagesCounterInsertAsManyAsPossible.incInt(allMessages)
                producerSendFailedMessagesCounterInsertAsManyAsPossible.incInt(failedMessages)
                producerSendDurationInsertAsManyAsPossible.observeNanos(executionNanos)
                producerSendBytesCounterInsertAsManyAsPossible.incLong(bytesSent)
            }
        }
    }

    private val producerSendCounterProhibited: CounterDataPoint = metrics.producerSendCounter
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendCounterUntilFirstFailure: CounterDataPoint = metrics.producerSendCounter
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendCounterInsertAsManyAsPossible: CounterDataPoint = metrics.producerSendCounter
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendMessagesCounterProhibited: CounterDataPoint = metrics.producerSendMessagesCounter
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendMessagesCounterUntilFirstFailure: CounterDataPoint = metrics.producerSendMessagesCounter
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendMessagesCounterInsertAsManyAsPossible: CounterDataPoint = metrics.producerSendMessagesCounter
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendFailedMessagesCounterProhibited: CounterDataPoint =
        metrics.producerSendFailedMessagesCounter
            .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendFailedMessagesCounterUntilFirstFailure: CounterDataPoint =
        metrics.producerSendFailedMessagesCounter
            .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendFailedMessagesCounterInsertAsManyAsPossible: CounterDataPoint =
        metrics.producerSendFailedMessagesCounter
            .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendDurationProhibited: DistributionDataPoint = metrics.producerSendDuration
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendDurationUntilFirstFailure: DistributionDataPoint = metrics.producerSendDuration
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendDurationInsertAsManyAsPossible: DistributionDataPoint = metrics.producerSendDuration
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendBytesCounterProhibited: CounterDataPoint = metrics.producerSendBytesCounter
        .labelValues(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendBytesCounterUntilFirstFailure: CounterDataPoint = metrics.producerSendBytesCounter
        .labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendBytesCounterInsertAsManyAsPossible: CounterDataPoint = metrics.producerSendBytesCounter
        .labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)
}
