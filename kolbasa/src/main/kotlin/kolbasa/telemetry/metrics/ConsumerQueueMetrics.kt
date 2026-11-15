package kolbasa.telemetry.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import kolbasa.schema.NodeId
import kolbasa.telemetry.metrics.Extensions.incInt
import kolbasa.telemetry.metrics.Extensions.incLong
import kolbasa.telemetry.metrics.Extensions.observeNanos

internal class ConsumerQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: MicrometerMetrics
) {

    fun consumerReceiveMetrics(
        receivedMessages: Int,
        unreadableMessages: Int,
        executionNanos: Long,
        bytesReceived: Long
    ) {
        consumerReceiveCounter.increment()
        consumerReceiveBytesCounter.incLong(bytesReceived)
        consumerReceiveMessagesCounter.incInt(receivedMessages)
        consumerReceiveUnreadableMessagesCounter.incInt(unreadableMessages)
        consumerReceiveDuration.observeNanos(executionNanos)
    }

    fun consumerDeleteMetrics(removedMessages: Int, executionNanos: Long) {
        consumerDeleteCounter.increment()
        consumerDeleteMessagesCounter.incInt(removedMessages)
        consumerDeleteDuration.observeNanos(executionNanos)
    }

    private val consumerReceiveCounter: Counter = metrics.consumerReceiveCounter
        .withTags(queueName, nodeId.id)
    private val consumerReceiveBytesCounter: Counter = metrics.consumerReceiveBytesCounter
        .withTags(queueName, nodeId.id)
    private val consumerReceiveMessagesCounter: Counter = metrics.consumerReceiveMessagesCounter
        .withTags(queueName, nodeId.id)
    private val consumerReceiveUnreadableMessagesCounter: Counter =
        metrics.consumerReceiveUnreadableMessagesCounter.withTags(queueName, nodeId.id)
    private val consumerReceiveDuration: Timer = metrics.consumerReceiveDuration
        .withTags(queueName, nodeId.id)

    private val consumerDeleteCounter: Counter = metrics.consumerDeleteCounter
        .withTags(queueName, nodeId.id)
    private val consumerDeleteMessagesCounter: Counter = metrics.consumerDeleteMessagesCounter
        .withTags(queueName, nodeId.id)
    private val consumerDeleteDuration: Timer = metrics.consumerDeleteDuration
        .withTags(queueName, nodeId.id)
}
