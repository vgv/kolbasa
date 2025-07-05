package kolbasa.stats.prometheus.metrics

import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId

internal object EmptyQueueMetrics : QueueMetrics {

    override fun usePreciseStringSize(): Boolean = false

    override fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
    }

    override fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
    }

    override fun consumerDeleteMetrics(nodeId: NodeId, removedMessages: Int, executionNanos: Long) {
    }

    override fun sweepMetrics(nodeId: NodeId, iterations: Int, removedMessages: Int, executionNanos: Long) {
    }
}
