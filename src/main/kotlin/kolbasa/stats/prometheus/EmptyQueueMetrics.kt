package kolbasa.stats.prometheus

import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import java.sql.Connection

internal object EmptyQueueMetrics : QueueMetrics {

    override fun usePreciseStringSize(): Boolean = false

    override fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        connection: Connection?
    ) {
    }

    override fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        connection: Connection?
    ) {
    }

    override fun consumerDeleteMetrics(nodeId: NodeId, removedMessages: Int, executionNanos: Long, connection: Connection?) {}

    override fun sweepMetrics(nodeId: NodeId, removedMessages: Int, executionNanos: Long, connection: Connection?) {}

    override fun mutatorMetrics(
        nodeId: NodeId,
        iterations: Int,
        mutatedMessages: Int,
        executionNanos: Long,
        byId: Boolean,
        connection: Connection?
    ) {
    }
}
