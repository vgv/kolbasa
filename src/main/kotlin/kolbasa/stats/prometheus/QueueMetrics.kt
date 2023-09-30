package kolbasa.stats.prometheus

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.producer.PartialInsert
import kolbasa.stats.prometheus.Extensions.incInt
import kolbasa.stats.prometheus.Extensions.incLong
import kolbasa.stats.prometheus.Extensions.observeNanos

internal class QueueMetrics(queueName: String) {

    fun producerSendMetrics(partialInsert: PartialInsert, rows: Int, failedMessages: Int, executionNanos: Long, approxBytes: Long) {
        when (partialInsert) {
            PartialInsert.PROHIBITED -> {
                producerSendCounter_Prohibited.inc()
                producerSendRowsCounter_Prohibited.incInt(rows)
                producerSendFailedRowsCounter_Prohibited.incInt(failedMessages)
                producerSendDuration_Prohibited.observeNanos(executionNanos)
                producerSendBytesCounter_Prohibited.incLong(approxBytes)
            }

            PartialInsert.UNTIL_FIRST_FAILURE -> {
                producerSendCounter_UntilFirstFailure.inc()
                producerSendRowsCounter_UntilFirstFailure.incInt(rows)
                producerSendFailedRowsCounter_UntilFirstFailure.incInt(failedMessages)
                producerSendDuration_UntilFirstFailure.observeNanos(executionNanos)
                producerSendBytesCounter_UntilFirstFailure.incLong(approxBytes)
            }

            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> {
                producerSendCounter_InsertAsManyAsPossible.inc()
                producerSendRowsCounter_InsertAsManyAsPossible.incInt(rows)
                producerSendFailedRowsCounter_InsertAsManyAsPossible.incInt(failedMessages)
                producerSendDuration_InsertAsManyAsPossible.observeNanos(executionNanos)
                producerSendBytesCounter_InsertAsManyAsPossible.incLong(approxBytes)
            }
        }
    }

    // Producer
    private val producerSendCounter_Prohibited: CounterDataPoint =
        PrometheusProducer.producerSendCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendCounter_UntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendCounter_InsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendRowsCounter_Prohibited: CounterDataPoint =
        PrometheusProducer.producerSendRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendRowsCounter_UntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendRowsCounter_InsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendFailedRowsCounter_Prohibited: CounterDataPoint =
        PrometheusProducer.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendFailedRowsCounter_UntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendFailedRowsCounter_InsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendDuration_Prohibited: DistributionDataPoint =
        PrometheusProducer.producerSendDuration.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendDuration_UntilFirstFailure: DistributionDataPoint =
        PrometheusProducer.producerSendDuration.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendDuration_InsertAsManyAsPossible: DistributionDataPoint =
        PrometheusProducer.producerSendDuration.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendBytesCounter_Prohibited: CounterDataPoint =
        PrometheusProducer.producerSendBytesCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendBytesCounter_UntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendBytesCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendBytesCounter_InsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendBytesCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)


    // Consumer
    val consumerReceiveCounter: CounterDataPoint =
        PrometheusConsumer.consumerReceiveCounter.labelValues(queueName)
    val consumerReceiveBytesCounter: CounterDataPoint =
        PrometheusConsumer.consumerReceiveBytesCounter.labelValues(queueName)
    val consumerReceiveRowsCounter: CounterDataPoint =
        PrometheusConsumer.consumerReceiveRowsCounter.labelValues(queueName)
    val consumerReceiveDuration: DistributionDataPoint =
        PrometheusConsumer.consumerReceiveDuration.labelValues(queueName)
    val consumerDeleteCounter: CounterDataPoint =
        PrometheusConsumer.consumerDeleteCounter.labelValues(queueName)
    val consumerDeleteRowsCounter: CounterDataPoint =
        PrometheusConsumer.consumerDeleteRowsCounter.labelValues(queueName)
    val consumerDeleteDuration: DistributionDataPoint =
        PrometheusConsumer.consumerDeleteDuration.labelValues(queueName)

    // Sweep
    val sweepCounter: CounterDataPoint =
        PrometheusSweep.sweepCounter.labelValues(queueName)
    val sweepIterationsCounter: CounterDataPoint =
        PrometheusSweep.sweepIterationsCounter.labelValues(queueName)
    val sweepOneIterationRowsRemovedCounter: CounterDataPoint =
        PrometheusSweep.sweepOneIterationRowsRemovedCounter.labelValues(queueName)
    val sweepOneIterationDuration: DistributionDataPoint =
        PrometheusSweep.sweepOneIterationDuration.labelValues(queueName)

}
