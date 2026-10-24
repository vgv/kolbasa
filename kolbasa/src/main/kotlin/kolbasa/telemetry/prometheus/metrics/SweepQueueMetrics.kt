package kolbasa.telemetry.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.schema.NodeId
import kolbasa.telemetry.prometheus.PrometheusConfig
import kolbasa.telemetry.prometheus.metrics.Extensions.incInt
import kolbasa.telemetry.prometheus.metrics.Extensions.observeNanos

internal class SweepQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    prometheusConfig: PrometheusConfig.Config
) {

    fun sweepMetrics(removedMessages: Int, executionNanos: Long) {
        sweepCounter.inc()
        sweepMessagesRemovedCounter.incInt(removedMessages)
        sweepDuration.observeNanos(executionNanos)
    }

    private val sweepCounter: CounterDataPoint = prometheusConfig.sweepCounter
        .labelValues(queueName, nodeId.id)
    private val sweepMessagesRemovedCounter: CounterDataPoint = prometheusConfig.sweepMessagesRemovedCounter
        .labelValues(queueName, nodeId.id)
    private val sweepDuration: DistributionDataPoint = prometheusConfig.sweepDuration
        .labelValues(queueName, nodeId.id)
}
