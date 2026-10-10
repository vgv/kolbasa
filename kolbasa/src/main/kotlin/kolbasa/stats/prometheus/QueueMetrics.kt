package kolbasa.stats.prometheus

import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import java.sql.Connection

internal interface QueueMetrics {

    fun usePreciseStringSize(): Boolean

    fun producerSendMetrics(
        nodeId: NodeId,
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        connection: Connection? = null
    )

    fun consumerReceiveMetrics(
        nodeId: NodeId,
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
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
