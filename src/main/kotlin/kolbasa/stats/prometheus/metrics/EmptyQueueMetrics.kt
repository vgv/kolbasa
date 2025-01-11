package kolbasa.stats.prometheus.metrics

import kolbasa.producer.PartialInsert

internal object EmptyQueueMetrics : QueueMetrics {

    override fun usePreciseStringSize(): Boolean = false

    override fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
    }

    override fun consumerReceiveMetrics(
        receivedRows: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
    }

    override fun consumerDeleteMetrics(removedRows: Int, executionNanos: Long) {
    }

    override fun sweepMetrics(iterations: Int, removedRows: Int, executionNanos: Long) {
    }
}
