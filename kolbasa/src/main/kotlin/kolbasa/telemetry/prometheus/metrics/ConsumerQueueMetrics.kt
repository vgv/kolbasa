package kolbasa.telemetry.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.schema.NodeId
import kolbasa.telemetry.prometheus.metrics.Extensions.incInt
import kolbasa.telemetry.prometheus.metrics.Extensions.incLong
import kolbasa.telemetry.prometheus.metrics.Extensions.observeNanos

internal class ConsumerQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: PrometheusMetrics
) {

    fun consumerReceiveMetrics(
        receivedMessages: Int,
        unreadableMessages: Int,
        executionNanos: Long,
        bytesReceived: Long
    ) {
        consumerReceiveCounter.inc()
        consumerReceiveBytesCounter.incLong(bytesReceived)
        consumerReceiveMessagesCounter.incInt(receivedMessages)
        consumerReceiveUnreadableMessagesCounter.incInt(unreadableMessages)
        consumerReceiveDuration.observeNanos(executionNanos)
    }

    fun consumerDeleteMetrics(removedMessages: Int, executionNanos: Long) {
        consumerDeleteCounter.inc()
        consumerDeleteMessagesCounter.incInt(removedMessages)
        consumerDeleteDuration.observeNanos(executionNanos)
    }

    private val consumerReceiveCounter: CounterDataPoint = metrics.consumerReceiveCounter
        .labelValues(queueName, nodeId.id)
    private val consumerReceiveBytesCounter: CounterDataPoint = metrics.consumerReceiveBytesCounter
        .labelValues(queueName, nodeId.id)
    private val consumerReceiveMessagesCounter: CounterDataPoint = metrics.consumerReceiveMessagesCounter
        .labelValues(queueName, nodeId.id)
    private val consumerReceiveUnreadableMessagesCounter: CounterDataPoint =
        metrics.consumerReceiveUnreadableMessagesCounter.labelValues(queueName, nodeId.id)
    private val consumerReceiveDuration: DistributionDataPoint = metrics.consumerReceiveDuration
        .labelValues(queueName, nodeId.id)

    private val consumerDeleteCounter: CounterDataPoint = metrics.consumerDeleteCounter
        .labelValues(queueName, nodeId.id)
    private val consumerDeleteMessagesCounter: CounterDataPoint = metrics.consumerDeleteMessagesCounter
        .labelValues(queueName, nodeId.id)
    private val consumerDeleteDuration: DistributionDataPoint = metrics.consumerDeleteDuration
        .labelValues(queueName, nodeId.id)
}
