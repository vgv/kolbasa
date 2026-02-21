package kolbasa.stats.prometheus.metrics

import kolbasa.inspector.MessageAge
import kolbasa.inspector.Messages
import kolbasa.schema.NodeId
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.Extensions.asSeconds
import kolbasa.stats.prometheus.metrics.Extensions.setLong

internal class CommonQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    prometheusConfig: PrometheusConfig.Config
) {

    val cacheKey = "$queueName-$nodeId"

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
        if (messageAge.oldest != null) {
            queueMessageAgesGaugeOldest.set(messageAge.oldest.asSeconds())
        }
        if (messageAge.newest != null) {
            queueMessageAgesGaugeNewest.set(messageAge.newest.asSeconds())
        }
        if (messageAge.oldestReady != null) {
            queueMessageAgesGaugeOldestReady.set(messageAge.oldestReady.asSeconds())
        }
    }

    private val queueMessagesGaugeScheduled = prometheusConfig.queueMessagesGauge
        .labelValues(queueName, "SCHEDULED", nodeId.id)
    private val queueMessagesGaugeReady = prometheusConfig.queueMessagesGauge
        .labelValues(queueName, "READY", nodeId.id)
    private val queueMessagesGaugeInFlight = prometheusConfig.queueMessagesGauge
        .labelValues(queueName, "IN_FLIGHT", nodeId.id)
    private val queueMessagesGaugeRetry = prometheusConfig.queueMessagesGauge
        .labelValues(queueName, "RETRY", nodeId.id)
    private val queueMessagesGaugeDead = prometheusConfig.queueMessagesGauge
        .labelValues(queueName, "DEAD", nodeId.id)

    private val queueMessageAgesGaugeOldest = prometheusConfig.queueMessageAgesGauge
        .labelValues(queueName, "OLDEST", nodeId.id)
    private val queueMessageAgesGaugeNewest = prometheusConfig.queueMessageAgesGauge
        .labelValues(queueName, "NEWEST", nodeId.id)
    private val queueMessageAgesGaugeOldestReady = prometheusConfig.queueMessageAgesGauge
        .labelValues(queueName, "OLDEST_READY", nodeId.id)

    private val queueSizeGauge = prometheusConfig.queueSizeGauge
        .labelValues(queueName, nodeId.id)
}
