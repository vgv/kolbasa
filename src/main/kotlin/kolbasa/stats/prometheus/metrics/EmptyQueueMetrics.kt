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
        receivedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
    }

    override fun consumerDeleteMetrics(removedMessages: Int, executionNanos: Long) {
    }

    override fun sweepMetrics(iterations: Int, removedMessages: Int, executionNanos: Long) {
    }
}
