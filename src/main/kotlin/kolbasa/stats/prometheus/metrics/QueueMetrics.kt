package kolbasa.stats.prometheus.metrics

import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId

internal interface QueueMetrics {

    fun usePreciseStringSize(): Boolean

    fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    )

    fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    )

    fun consumerDeleteMetrics(
        nodeId: NodeId,
        removedMessages: Int,
        executionNanos: Long
    )

    fun sweepMetrics(
        nodeId: NodeId,
        removedMessages: Int,
        executionNanos: Long
    )

    fun mutatorMetrics(
        nodeId: NodeId,
        iterations: Int,
        mutatedMessages: Int,
        executionNanos: Long,
        byId: Boolean
    )
}
