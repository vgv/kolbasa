package kolbasa.telemetry.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.schema.NodeId
import kolbasa.telemetry.prometheus.metrics.Extensions.incInt
import kolbasa.telemetry.prometheus.metrics.Extensions.observeNanos

internal class SweepQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: PrometheusMetrics
) {

    fun sweepMetrics(removedMessages: Int, executionNanos: Long) {
        sweepCounter.inc()
        sweepMessagesRemovedCounter.incInt(removedMessages)
        sweepDuration.observeNanos(executionNanos)
    }

    private val sweepCounter: CounterDataPoint = metrics.sweepCounter
        .labelValues(queueName, nodeId.id)
    private val sweepMessagesRemovedCounter: CounterDataPoint = metrics.sweepMessagesRemovedCounter
        .labelValues(queueName, nodeId.id)
    private val sweepDuration: DistributionDataPoint = metrics.sweepDuration
        .labelValues(queueName, nodeId.id)
}
