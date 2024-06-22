package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import io.prometheus.metrics.core.datapoints.GaugeDataPoint
import kolbasa.producer.PartialInsert
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.Extensions.incInt
import kolbasa.stats.prometheus.metrics.Extensions.incLong
import kolbasa.stats.prometheus.metrics.Extensions.observeNanos
import kolbasa.stats.prometheus.queuesize.QueueSizeCache

internal class PrometheusQueueMetrics(
    private val queueName: String,
    private val prometheusConfig: PrometheusConfig.Config
) : QueueMetrics {

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
        val queueSize = QueueSizeCache.get(queueName, queueSizeMeasureInterval, queueSizeCalcFunc)
        producerQueueSizeGauge.set(queueSize.toDouble())
    }

    private val producerSendCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendRowsCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendRowsCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendFailedRowsCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendFailedRowsCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendFailedRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendFailedRowsCounter.labelValues(
            queueName,
            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name
        )

    private val producerSendDurationProhibited: DistributionDataPoint =
        prometheusConfig.producerSendDuration.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendDurationUntilFirstFailure: DistributionDataPoint =
        prometheusConfig.producerSendDuration.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendDurationInsertAsManyAsPossible: DistributionDataPoint =
        prometheusConfig.producerSendDuration.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendBytesCounterProhibited: CounterDataPoint =
        prometheusConfig.producerSendBytesCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendBytesCounterUntilFirstFailure: CounterDataPoint =
        prometheusConfig.producerSendBytesCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendBytesCounterInsertAsManyAsPossible: CounterDataPoint =
        prometheusConfig.producerSendBytesCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerQueueSizeGauge: GaugeDataPoint =
        prometheusConfig.producerQueueSizeGauge.labelValues(queueName)


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
        val queueSize = QueueSizeCache.get(queueName, queueSizeMeasureInterval, queueSizeCalcFunc)
        consumerQueueSizeGauge.set(queueSize.toDouble())
    }

    override fun consumerDeleteMetrics(removedRows: Int, executionNanos: Long) {
        consumerDeleteCounter.inc()
        consumerDeleteRowsCounter.incInt(removedRows)
        consumerDeleteDuration.observeNanos(executionNanos)
    }

    private val consumerReceiveCounter: CounterDataPoint =
        prometheusConfig.consumerReceiveCounter.labelValues(queueName)
    private val consumerReceiveBytesCounter: CounterDataPoint =
        prometheusConfig.consumerReceiveBytesCounter.labelValues(queueName)
    private val consumerReceiveRowsCounter: CounterDataPoint =
        prometheusConfig.consumerReceiveRowsCounter.labelValues(queueName)
    private val consumerReceiveDuration: DistributionDataPoint =
        prometheusConfig.consumerReceiveDuration.labelValues(queueName)
    private val consumerDeleteCounter: CounterDataPoint =
        prometheusConfig.consumerDeleteCounter.labelValues(queueName)
    private val consumerDeleteRowsCounter: CounterDataPoint =
        prometheusConfig.consumerDeleteRowsCounter.labelValues(queueName)
    private val consumerDeleteDuration: DistributionDataPoint =
        prometheusConfig.consumerDeleteDuration.labelValues(queueName)
    private val consumerQueueSizeGauge: GaugeDataPoint =
        prometheusConfig.consumerQueueSizeGauge.labelValues(queueName)

    // ------------------------------------------------------------------------------
    // Sweep
    override fun sweepMetrics(iterations: Int, removedRows: Int, executionNanos: Long) {
        sweepCounter.inc()
        sweepIterationsCounter.incInt(iterations)
        sweepRowsRemovedCounter.incInt(removedRows)
        sweepDuration.observeNanos(executionNanos)
    }

    private val sweepCounter: CounterDataPoint =
        prometheusConfig.sweepCounter.labelValues(queueName)
    private val sweepIterationsCounter: CounterDataPoint =
        prometheusConfig.sweepIterationsCounter.labelValues(queueName)
    private val sweepRowsRemovedCounter: CounterDataPoint =
        prometheusConfig.sweepRowsRemovedCounter.labelValues(queueName)
    private val sweepDuration: DistributionDataPoint =
        prometheusConfig.sweepDuration.labelValues(queueName)

}
