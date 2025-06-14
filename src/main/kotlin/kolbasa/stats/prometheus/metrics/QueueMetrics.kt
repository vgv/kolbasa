package kolbasa.stats.prometheus.metrics

import kolbasa.producer.PartialInsert

internal interface QueueMetrics {

    fun usePreciseStringSize(): Boolean

    fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    )

    fun consumerReceiveMetrics(
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    )

    fun consumerDeleteMetrics(removedMessages: Int, executionNanos: Long)

    fun sweepMetrics(iterations: Int, removedMessages: Int, executionNanos: Long)

}
