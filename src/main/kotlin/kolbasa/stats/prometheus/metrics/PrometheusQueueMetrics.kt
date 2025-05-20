package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import io.prometheus.metrics.core.datapoints.GaugeDataPoint
import kolbasa.producer.PartialInsert
import kolbasa.schema.ServerId
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.Extensions.incInt
import kolbasa.stats.prometheus.metrics.Extensions.incLong
import kolbasa.stats.prometheus.metrics.Extensions.observeNanos
import kolbasa.stats.prometheus.queuesize.QueueSizeCache

internal class PrometheusQueueMetrics(
    private val queueName: String,
    private val serverId: ServerId,
    private val prometheusConfig: PrometheusConfig.Config,
) : QueueMetrics {

    override fun usePreciseStringSize(): Boolean {
        return prometheusConfig.preciseStringSize
    }

    // ------------------------------------------------------------------------------
    // Producer
    override fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
        when (partialInsert) {
            PartialInsert.PROHIBITED -> {
                producerSendCounterProhibited.inc()
                producerSendRowsCounterProhibited.incInt(allMessages)
                producerSendFailedRowsCounterProhibited.incInt(failedMessages)
                producerSendDurationProhibited.observeNanos(executionNanos)
                producerSendBytesCounterProhibited.incLong(approxBytes)
            }

            PartialInsert.UNTIL_FIRST_FAILURE -> {
                producerSendCounterUntilFirstFailure.inc()
                producerSendRowsCounterUntilFirstFailure.incInt(allMessages)
                producerSendFailedRowsCounterUntilFirstFailure.incInt(failedMessages)
                producerSendDurationUntilFirstFailure.observeNanos(executionNanos)
                producerSendBytesCounterUntilFirstFailure.incLong(approxBytes)
            }

            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> {
                producerSendCounterInsertAsManyAsPossible.inc()
                producerSendRowsCounterInsertAsManyAsPossible.incInt(allMessages)
                producerSendFailedRowsCounterInsertAsManyAsPossible.incInt(failedMessages)
                producerSendDurationInsertAsManyAsPossible.observeNanos(executionNanos)
                producerSendBytesCounterInsertAsManyAsPossible.incLong(approxBytes)
            }
        }

        // Queue size
        // We use internal caching to prevent too frequent calls to the database
        val queueSizeMeasureInterval = prometheusConfig.customQueueSizeMeasureInterval[queueName]
            ?: PrometheusConfig.Config.DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL
        val queueSize = QueueSizeCache.get(queueName, serverId, queueSizeMeasureInterval, queueSizeCalcFunc)
        producerQueueSizeGauge.set(queueSize.toDouble())
    }

    private val producerSendCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendCounter.labelValues(queueName, PartialInsert.PROHIBITED.name, serverId)
    private val producerSendCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, serverId)
    private val producerSendCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, serverId)

    private val producerSendRowsCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name, serverId)
    private val producerSendRowsCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, serverId)
    private val producerSendRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, serverId)

    private val producerSendFailedRowsCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name, serverId)
    private val producerSendFailedRowsCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, serverId)
    private val producerSendFailedRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendFailedRowsCounter.labelValues(
            queueName,
            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name,
            serverId
        )

    private val producerSendDurationProhibited: DistributionDataPoint =
        prometheusConfig.producerSendDuration.labelValues(queueName, PartialInsert.PROHIBITED.name, serverId)
    private val producerSendDurationUntilFirstFailure: DistributionDataPoint =
        prometheusConfig.producerSendDuration.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, serverId)
    private val producerSendDurationInsertAsManyAsPossible: DistributionDataPoint =
        prometheusConfig.producerSendDuration.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, serverId)

    private val producerSendBytesCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendBytesCounter.labelValues(queueName, PartialInsert.PROHIBITED.name, serverId)
    private val producerSendBytesCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendBytesCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, serverId)
    private val producerSendBytesCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendBytesCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, serverId)

    private val producerQueueSizeGauge: GaugeDataPoint =
        prometheusConfig.producerQueueSizeGauge.labelValues(queueName, serverId)


    // ------------------------------------------------------------------------------
    // Consumer
    override fun consumerReceiveMetrics(
        receivedRows: Int,
        executionNanos: Long,
        approxBytes: Long,
        queueSizeCalcFunc: () -> Long
    ) {
        consumerReceiveCounter.inc()
        consumerReceiveBytesCounter.incLong(approxBytes)
        consumerReceiveRowsCounter.incInt(receivedRows)
        consumerReceiveDuration.observeNanos(executionNanos)

        // Queue size
        // We use internal caching to prevent too frequent calls to the database
        val queueSizeMeasureInterval = prometheusConfig.customQueueSizeMeasureInterval[queueName]
            ?: PrometheusConfig.Config.DEFAULT_QUEUE_SIZE_MEASURE_INTERVAL
        val queueSize = QueueSizeCache.get(queueName, serverId, queueSizeMeasureInterval, queueSizeCalcFunc)
        consumerQueueSizeGauge.set(queueSize.toDouble())
    }

    override fun consumerDeleteMetrics(removedRows: Int, executionNanos: Long) {
        consumerDeleteCounter.inc()
        consumerDeleteRowsCounter.incInt(removedRows)
        consumerDeleteDuration.observeNanos(executionNanos)
    }

    private val consumerReceiveCounter: CounterDataPoint =
        prometheusConfig.consumerReceiveCounter.labelValues(queueName, serverId)
    private val consumerReceiveBytesCounter: CounterDataPoint =
        prometheusConfig.consumerReceiveBytesCounter.labelValues(queueName, serverId)
    private val consumerReceiveRowsCounter: CounterDataPoint =
        prometheusConfig.consumerReceiveRowsCounter.labelValues(queueName, serverId)
    private val consumerReceiveDuration: DistributionDataPoint =
        prometheusConfig.consumerReceiveDuration.labelValues(queueName, serverId)
    private val consumerDeleteCounter: CounterDataPoint =
        prometheusConfig.consumerDeleteCounter.labelValues(queueName, serverId)
    private val consumerDeleteRowsCounter: CounterDataPoint =
        prometheusConfig.consumerDeleteRowsCounter.labelValues(queueName, serverId)
    private val consumerDeleteDuration: DistributionDataPoint =
        prometheusConfig.consumerDeleteDuration.labelValues(queueName, serverId)
    private val consumerQueueSizeGauge: GaugeDataPoint =
        prometheusConfig.consumerQueueSizeGauge.labelValues(queueName, serverId)

    // ------------------------------------------------------------------------------
    // Sweep
    override fun sweepMetrics(iterations: Int, removedRows: Int, executionNanos: Long) {
        sweepCounter.inc()
        sweepIterationsCounter.incInt(iterations)
        sweepRowsRemovedCounter.incInt(removedRows)
        sweepDuration.observeNanos(executionNanos)
    }

    private val sweepCounter: CounterDataPoint =
        prometheusConfig.sweepCounter.labelValues(queueName, serverId)
    private val sweepIterationsCounter: CounterDataPoint =
        prometheusConfig.sweepIterationsCounter.labelValues(queueName, serverId)
    private val sweepRowsRemovedCounter: CounterDataPoint =
        prometheusConfig.sweepRowsRemovedCounter.labelValues(queueName, serverId)
    private val sweepDuration: DistributionDataPoint =
        prometheusConfig.sweepDuration.labelValues(queueName, serverId)

}
