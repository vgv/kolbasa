package kolbasa.stats.prometheus

import kolbasa.inspector.MessageAge
import kolbasa.inspector.Messages
import kolbasa.inspector.connection.ConnectionAwareDatabaseInspector
import kolbasa.producer.PartialInsert
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import kolbasa.stats.prometheus.metrics.*
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

internal class PrometheusQueueMetrics(
    private val queue: Queue<*>,
    private val prometheusConfig: PrometheusConfig.Config
) : QueueMetrics {

    private val queueName = queue.name

    private val common: ConcurrentMap<NodeId, CommonQueueMetrics> = ConcurrentHashMap()
    private val producer: ConcurrentMap<NodeId, ProducerQueueMetrics> = ConcurrentHashMap()
    private val consumer: ConcurrentMap<NodeId, ConsumerQueueMetrics> = ConcurrentHashMap()
    private val sweep: ConcurrentMap<NodeId, SweepQueueMetrics> = ConcurrentHashMap()
    private val mutator: ConcurrentMap<NodeId, MutatorQueueMetrics> = ConcurrentHashMap()

    override fun usePreciseStringSize(): Boolean {
        return prometheusConfig.preciseStringSize
    }

    override fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        connection: Connection?
    ) {
        val producerQueueMetrics = producer.computeIfAbsent(nodeId) { _ ->
            ProducerQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        producerQueueMetrics.producerSendMetrics(
            partialInsert = partialInsert,
            allMessages = allMessages,
            failedMessages = failedMessages,
            executionNanos = executionNanos,
            approxBytes = approxBytes
        )

        // Update common metrics
        if (connection != null) {
            sendCommonMetrics(connection, nodeId)
        }
    }

    override fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        connection: Connection?
    ) {
        val consumerQueueMetrics = consumer.computeIfAbsent(nodeId) { _ ->
            ConsumerQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        consumerQueueMetrics.consumerReceiveMetrics(
            receivedMessages = receivedMessages,
            executionNanos = executionNanos,
            approxBytes = approxBytes
        )

        // Update common metrics
        if (connection != null) {
            sendCommonMetrics(connection, nodeId)
        }
    }

    override fun consumerDeleteMetrics(nodeId: NodeId, removedMessages: Int, executionNanos: Long, connection: Connection?) {
        val consumerQueueMetrics = consumer.computeIfAbsent(nodeId) { _ ->
            ConsumerQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        consumerQueueMetrics.consumerDeleteMetrics(
            removedMessages = removedMessages,
            executionNanos = executionNanos
        )

        // Update common metrics
        if (connection != null) {
            sendCommonMetrics(connection, nodeId)
        }
    }

    override fun sweepMetrics(nodeId: NodeId, removedMessages: Int, executionNanos: Long, connection: Connection?) {
        val sweepQueueMetrics = sweep.computeIfAbsent(nodeId) { _ ->
            SweepQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        sweepQueueMetrics.sweepMetrics(
            removedMessages = removedMessages,
            executionNanos = executionNanos
        )

        // Update common metrics
        if (connection != null) {
            sendCommonMetrics(connection, nodeId)
        }
    }

    override fun mutatorMetrics(
        nodeId: NodeId,
        iterations: Int,
        mutatedMessages: Int,
        executionNanos: Long,
        byId: Boolean,
        connection: Connection?
    ) {
        val mutatorQueueMetrics = mutator.computeIfAbsent(nodeId) { _ ->
            MutatorQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        mutatorQueueMetrics.mutatorMetrics(
            iterations = iterations,
            mutatedMessages = mutatedMessages,
            executionNanos = executionNanos,
            byId = byId
        )

        // Update common metrics
        if (connection != null) {
            sendCommonMetrics(connection, nodeId)
        }
    }

    private fun sendCommonMetrics(connection: Connection, nodeId: NodeId) {
        val commonQueueMetrics = common.computeIfAbsent(nodeId) { _ ->
            CommonQueueMetrics(queueName, nodeId, prometheusConfig)
        }

        val cacheValue = getCachedValue(connection, queue, commonQueueMetrics.cacheKey)

        commonQueueMetrics.queueMetrics(
            messages = cacheValue.messages,
            queueSizeBytes = cacheValue.queueSizeBytes,
            messageAge = cacheValue.messageAge
        )
    }

    private companion object {

        private const val CACHE_LIFETIME_MILLIS = 60_000 // 1 minute

        data class CacheValue(
            val messages: Messages,
            val queueSizeBytes: Long,
            val messageAge: MessageAge,
            val expiration: Long
        )

        val inspector = ConnectionAwareDatabaseInspector()
        val cache: ConcurrentMap<String, CacheValue> = ConcurrentHashMap()

        fun getCachedValue(connection: Connection, queue: Queue<*>, cacheKey: String): CacheValue {
            var value = cache[cacheKey]
            if (value != null && System.currentTimeMillis() < value.expiration) {
                return value
            }

            // Cache miss or expired, fetch new values
            val messages = inspector.count(connection, queue)
            val queueSizeBytes = inspector.size(connection, queue)
            val messageAge = inspector.messageAge(connection, queue)

            value = CacheValue(messages, queueSizeBytes, messageAge, System.currentTimeMillis() + CACHE_LIFETIME_MILLIS)
            cache[cacheKey] = value
            return value
        }
    }
}

