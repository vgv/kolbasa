package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.Kolbasa
import kolbasa.producer.PartialInsert
import kolbasa.stats.prometheus.metrics.Extensions.incInt
import kolbasa.stats.prometheus.metrics.Extensions.incLong
import kolbasa.stats.prometheus.metrics.Extensions.observeNanos

internal class QueueMetrics(queueName: String) {

    // ------------------------------------------------------------------------------
    // Producer
    fun producerSendMetrics(partialInsert: PartialInsert, allMessages: Int, failedMessages: Int, executionNanos: Long, approxBytes: Long) {
        if (!Kolbasa.prometheusConfig.enabled) {
            return
        }

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
    }

    private val producerSendCounterProhibited: CounterDataPoint =
        PrometheusProducerMetrics.producerSendCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducerMetrics.producerSendCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducerMetrics.producerSendCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendRowsCounterProhibited: CounterDataPoint =
        PrometheusProducerMetrics.producerSendRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendRowsCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducerMetrics.producerSendRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducerMetrics.producerSendRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendFailedRowsCounterProhibited: CounterDataPoint =
        PrometheusProducerMetrics.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendFailedRowsCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducerMetrics.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendFailedRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducerMetrics.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendDurationProhibited: DistributionDataPoint =
        PrometheusProducerMetrics.producerSendDuration.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendDurationUntilFirstFailure: DistributionDataPoint =
        PrometheusProducerMetrics.producerSendDuration.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendDurationInsertAsManyAsPossible: DistributionDataPoint =
        PrometheusProducerMetrics.producerSendDuration.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendBytesCounterProhibited: CounterDataPoint =
        PrometheusProducerMetrics.producerSendBytesCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendBytesCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducerMetrics.producerSendBytesCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendBytesCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducerMetrics.producerSendBytesCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)


    // ------------------------------------------------------------------------------
    // Consumer
    fun consumerReceiveMetrics(receivedRows: Int, executionNanos: Long, approxBytes: Long) {
        if (!Kolbasa.prometheusConfig.enabled) {
            return
        }

        consumerReceiveCounter.inc()
        consumerReceiveBytesCounter.incLong(approxBytes)
        consumerReceiveRowsCounter.incInt(receivedRows)
        consumerReceiveDuration.observeNanos(executionNanos)
    }

    fun consumerDeleteMetrics(removedRows: Int, executionNanos: Long) {
        if (!Kolbasa.prometheusConfig.enabled) {
            return
        }

        consumerDeleteCounter.inc()
        consumerDeleteRowsCounter.incInt(removedRows)
        consumerDeleteDuration.observeNanos(executionNanos)
    }

    private val consumerReceiveCounter: CounterDataPoint =
        PrometheusConsumerMetrics.consumerReceiveCounter.labelValues(queueName)
    private val consumerReceiveBytesCounter: CounterDataPoint =
        PrometheusConsumerMetrics.consumerReceiveBytesCounter.labelValues(queueName)
    private val consumerReceiveRowsCounter: CounterDataPoint =
        PrometheusConsumerMetrics.consumerReceiveRowsCounter.labelValues(queueName)
    private val consumerReceiveDuration: DistributionDataPoint =
        PrometheusConsumerMetrics.consumerReceiveDuration.labelValues(queueName)
    private val consumerDeleteCounter: CounterDataPoint =
        PrometheusConsumerMetrics.consumerDeleteCounter.labelValues(queueName)
    private val consumerDeleteRowsCounter: CounterDataPoint =
        PrometheusConsumerMetrics.consumerDeleteRowsCounter.labelValues(queueName)
    private val consumerDeleteDuration: DistributionDataPoint =
        PrometheusConsumerMetrics.consumerDeleteDuration.labelValues(queueName)

    // ------------------------------------------------------------------------------
    // Sweep
    fun sweepMetrics(iterations: Int, removedRows: Int, executionNanos: Long) {
        if (!Kolbasa.prometheusConfig.enabled) {
            return
        }

        sweepCounter.inc()
        sweepIterationsCounter.incInt(iterations)
        sweepRowsRemovedCounter.incInt(removedRows)
        sweepDuration.observeNanos(executionNanos)
    }

    private val sweepCounter: CounterDataPoint =
        PrometheusSweepMetrics.sweepCounter.labelValues(queueName)
    private val sweepIterationsCounter: CounterDataPoint =
        PrometheusSweepMetrics.sweepIterationsCounter.labelValues(queueName)
    private val sweepRowsRemovedCounter: CounterDataPoint =
        PrometheusSweepMetrics.sweepRowsRemovedCounter.labelValues(queueName)
    private val sweepDuration: DistributionDataPoint =
        PrometheusSweepMetrics.sweepDuration.labelValues(queueName)

}
