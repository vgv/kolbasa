package kolbasa.telemetry.prometheus

import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import kolbasa.utils.BytesCounter
import java.sql.Connection

internal interface QueueMetrics {

    // A counter for the bytes of one call. When the metrics are off, it counts nothing
    fun bytesCounter(): BytesCounter

    fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        bytesSent: Long,
        connection: Connection? = null
    )

    fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        unreadableMessages: Int,
        executionNanos: Long,
        bytesReceived: Long,
        connection: Connection? = null
    )

    fun consumerDeleteMetrics(
        nodeId: NodeId,
        removedMessages: Int,
        executionNanos: Long,
        connection: Connection? = null
    )

    fun sweepMetrics(
        nodeId: NodeId,
        removedMessages: Int,
        executionNanos: Long,
        connection: Connection? = null
    )

    fun mutatorMetrics(
        nodeId: NodeId,
        iterations: Int,
        mutatedMessages: Int,
        executionNanos: Long,
        byId: Boolean,
        connection: Connection? = null
    )
}
