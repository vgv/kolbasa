package kolbasa.telemetry.prometheus.metrics

import kolbasa.inspector.MessageAge
import kolbasa.inspector.Messages
import kolbasa.schema.NodeId
import kolbasa.telemetry.prometheus.metrics.Extensions.asSeconds
import kolbasa.telemetry.prometheus.metrics.Extensions.setLong

internal class CommonQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: PrometheusMetrics
) {

    fun queueMetrics(
        messages: Messages,
        queueSizeBytes: Long,
        messageAge: MessageAge
    ) {
        // queue messages count (scheduled, ready, in-flight, retry, dead)
        queueMessagesGaugeScheduled.setLong(messages.scheduled)
        queueMessagesGaugeReady.setLong(messages.ready)
        queueMessagesGaugeInFlight.setLong(messages.inFlight)
        queueMessagesGaugeRetry.setLong(messages.retry)
        queueMessagesGaugeDead.setLong(messages.dead)

        // queue size in bytes
        queueSizeGauge.setLong(queueSizeBytes)

        // ages
        queueMessageAgesGaugeOldest.set(messageAge.oldest?.asSeconds() ?: 0.0)
        queueMessageAgesGaugeNewest.set(messageAge.newest?.asSeconds() ?: 0.0)
        queueMessageAgesGaugeOldestReady.set(messageAge.oldestReady?.asSeconds() ?: 0.0)
    }

    private val queueMessagesGaugeScheduled = metrics.queueMessagesGauge
        .labelValues(queueName, "SCHEDULED", nodeId.id)
    private val queueMessagesGaugeReady = metrics.queueMessagesGauge
        .labelValues(queueName, "READY", nodeId.id)
    private val queueMessagesGaugeInFlight = metrics.queueMessagesGauge
        .labelValues(queueName, "IN_FLIGHT", nodeId.id)
    private val queueMessagesGaugeRetry = metrics.queueMessagesGauge
        .labelValues(queueName, "RETRY", nodeId.id)
    private val queueMessagesGaugeDead = metrics.queueMessagesGauge
        .labelValues(queueName, "DEAD", nodeId.id)

    private val queueMessageAgesGaugeOldest = metrics.queueMessageAgesGauge
        .labelValues(queueName, "OLDEST", nodeId.id)
    private val queueMessageAgesGaugeNewest = metrics.queueMessageAgesGauge
        .labelValues(queueName, "NEWEST", nodeId.id)
    private val queueMessageAgesGaugeOldestReady = metrics.queueMessageAgesGauge
        .labelValues(queueName, "OLDEST_READY", nodeId.id)

    private val queueSizeGauge = metrics.queueSizeGauge
        .labelValues(queueName, nodeId.id)
}
