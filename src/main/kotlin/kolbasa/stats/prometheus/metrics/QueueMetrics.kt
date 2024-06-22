package kolbasa.stats.prometheus.metrics

import kolbasa.producer.PartialInsert

internal interface QueueMetrics {

    fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    )

    fun consumerReceiveMetrics(
        receivedRows: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    )

    fun consumerDeleteMetrics(removedRows: Int, executionNanos: Long)

    fun sweepMetrics(iterations: Int, removedRows: Int, executionNanos: Long)

}
