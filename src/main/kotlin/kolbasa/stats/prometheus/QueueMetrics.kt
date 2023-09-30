package kolbasa.stats.prometheus

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram
import kolbasa.Kolbasa
import kolbasa.producer.PartialInsert
import kolbasa.stats.prometheus.Extensions.incInt
import kolbasa.stats.prometheus.Extensions.incLong
import kolbasa.stats.prometheus.Extensions.observeNanos

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
        PrometheusProducer.producerSendCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendRowsCounterProhibited: CounterDataPoint =
        PrometheusProducer.producerSendRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendRowsCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendFailedRowsCounterProhibited: CounterDataPoint =
        PrometheusProducer.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendFailedRowsCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendFailedRowsCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendFailedRowsCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendDurationProhibited: DistributionDataPoint =
        PrometheusProducer.producerSendDuration.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendDurationUntilFirstFailure: DistributionDataPoint =
        PrometheusProducer.producerSendDuration.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendDurationInsertAsManyAsPossible: DistributionDataPoint =
        PrometheusProducer.producerSendDuration.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)

    private val producerSendBytesCounterProhibited: CounterDataPoint =
        PrometheusProducer.producerSendBytesCounter.labelValues(queueName, PartialInsert.PROHIBITED.name)
    private val producerSendBytesCounterUntilFirstFailure: CounterDataPoint =
        PrometheusProducer.producerSendBytesCounter.labelValues(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name)
    private val producerSendBytesCounterInsertAsManyAsPossible: CounterDataPoint =
        PrometheusProducer.producerSendBytesCounter.labelValues(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name)


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
        PrometheusConsumer.consumerReceiveCounter.labelValues(queueName)
    private val consumerReceiveBytesCounter: CounterDataPoint =
        PrometheusConsumer.consumerReceiveBytesCounter.labelValues(queueName)
    private val consumerReceiveRowsCounter: CounterDataPoint =
        PrometheusConsumer.consumerReceiveRowsCounter.labelValues(queueName)
    private val consumerReceiveDuration: DistributionDataPoint =
        PrometheusConsumer.consumerReceiveDuration.labelValues(queueName)
    private val consumerDeleteCounter: CounterDataPoint =
        PrometheusConsumer.consumerDeleteCounter.labelValues(queueName)
    private val consumerDeleteRowsCounter: CounterDataPoint =
        PrometheusConsumer.consumerDeleteRowsCounter.labelValues(queueName)
    private val consumerDeleteDuration: DistributionDataPoint =
        PrometheusConsumer.consumerDeleteDuration.labelValues(queueName)

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
        PrometheusSweep.sweepCounter.labelValues(queueName)
    private val sweepIterationsCounter: CounterDataPoint =
        PrometheusSweep.sweepIterationsCounter.labelValues(queueName)
    private val sweepRowsRemovedCounter: CounterDataPoint =
        PrometheusSweep.sweepRowsRemovedCounter.labelValues(queueName)
    private val sweepDuration: DistributionDataPoint =
        PrometheusSweep.sweepDuration.labelValues(queueName)

    private object PrometheusProducer {
        val producerSendCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send")
            .help("Amount of producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(Kolbasa.prometheusConfig.registry)

        val producerSendRowsCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_rows")
            .help("Amount of all (successful and failed) rows sent by producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(Kolbasa.prometheusConfig.registry)

        val producerSendFailedRowsCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_rows_failed")
            .help("Amount of failed rows sent by producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(Kolbasa.prometheusConfig.registry)

        val producerSendDuration: Histogram = Histogram.builder()
            .name("kolbasa_producer_send_duration_seconds")
            .help("Producer send() calls duration")
            .labelNames("queue", "partial_insert_type")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(Kolbasa.prometheusConfig.registry)

        val producerSendBytesCounter: Counter = Counter.builder()
            .name("kolbasa_producer_send_bytes")
            .help("Amount of bytes sent by producer send() calls")
            .labelNames("queue", "partial_insert_type")
            .register(Kolbasa.prometheusConfig.registry)
    }

    private object PrometheusConsumer {
        // Receive
        val consumerReceiveCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive")
            .help("Amount of consumer receive() calls")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val consumerReceiveBytesCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive_bytes")
            .help("Amount of bytes read by consumer receive() calls")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val consumerReceiveRowsCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_receive_rows")
            .help("Amount of rows received by consumer receive() calls")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val consumerReceiveDuration: Histogram = Histogram.builder()
            .name("kolbasa_consumer_receive_duration_seconds")
            .help("Consumer receive() calls duration")
            .labelNames("queue")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(Kolbasa.prometheusConfig.registry)

        // Delete
        val consumerDeleteCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_delete")
            .help("Amount of consumer delete() calls")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val consumerDeleteRowsCounter: Counter = Counter.builder()
            .name("kolbasa_consumer_delete_rows")
            .help("Amount of rows removed by consumer delete() calls")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val consumerDeleteDuration: Histogram = Histogram.builder()
            .name("kolbasa_consumer_delete_duration_seconds")
            .help("Consumer delete() calls duration")
            .labelNames("queue")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(Kolbasa.prometheusConfig.registry)
    }

    private object PrometheusSweep {
        val sweepCounter: Counter = Counter.builder()
            .name("kolbasa_sweep")
            .help("Number of sweeps")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val sweepIterationsCounter: Counter = Counter.builder()
            .name("kolbasa_sweep_iterations")
            .help("Number of sweep iterations (every sweep can have multiple iterations)")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val sweepRowsRemovedCounter: Counter = Counter.builder()
            .name("kolbasa_sweep_removed_rows")
            .help("Number of rows removed by sweep")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val sweepDuration: Histogram = Histogram.builder()
            .name("kolbasa_sweep_duration_seconds")
            .help("Sweep duration")
            .labelNames("queue")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(Kolbasa.prometheusConfig.registry)
    }
}
