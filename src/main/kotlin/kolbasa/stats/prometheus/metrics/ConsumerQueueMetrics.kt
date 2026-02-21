package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.schema.NodeId
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.Extensions.incInt
import kolbasa.stats.prometheus.metrics.Extensions.incLong
import kolbasa.stats.prometheus.metrics.Extensions.observeNanos

internal class ConsumerQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    prometheusConfig: PrometheusConfig.Config
) {

    fun consumerReceiveMetrics(
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long
    ) {
        consumerReceiveCounter.inc()
        consumerReceiveBytesCounter.incLong(approxBytes)
        consumerReceiveMessagesCounter.incInt(receivedMessages)
        consumerReceiveDuration.observeNanos(executionNanos)
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
}
