package kolbasa.telemetry.prometheus

import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import kolbasa.utils.BytesCounter
import java.sql.Connection

internal object EmptyQueueMetrics : QueueMetrics {

    override fun bytesCounter(): BytesCounter = BytesCounter.None

    override fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        bytesSent: Long,
        connection: Connection?
    ) {
    }

    override fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        unreadableMessages: Int,
        executionNanos: Long,
        bytesReceived: Long,
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
