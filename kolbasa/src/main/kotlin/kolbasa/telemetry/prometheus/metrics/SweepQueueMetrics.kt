package kolbasa.telemetry.prometheus.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import kolbasa.schema.NodeId
import kolbasa.telemetry.prometheus.metrics.Extensions.incInt
import kolbasa.telemetry.prometheus.metrics.Extensions.observeNanos

internal class SweepQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: PrometheusMetrics
) {

    fun sweepMetrics(removedMessages: Int, executionNanos: Long) {
        sweepCounter.increment()
        sweepMessagesRemovedCounter.incInt(removedMessages)
        sweepDuration.observeNanos(executionNanos)
    }

    private val sweepCounter: Counter = metrics.sweepCounter
        .withTags(queueName, nodeId.id)
    private val sweepMessagesRemovedCounter: Counter = metrics.sweepMessagesRemovedCounter
        .withTags(queueName, nodeId.id)
    private val sweepDuration: Timer = metrics.sweepDuration
        .withTags(queueName, nodeId.id)
}
