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

        val sweepOneIterationRowsRemovedCounter: Counter = Counter.builder()
            .name("kolbasa_sweep_iteration_removed_rows")
            .help("Number of rows removed by one sweep iteration")
            .labelNames("queue")
            .register(Kolbasa.prometheusConfig.registry)

        val sweepOneIterationDuration: Histogram = Histogram.builder()
            .name("kolbasa_sweep_iteration_duration_seconds")
            .help("One sweep iteration duration")
            .labelNames("queue")
            .classicOnly()
            .classicUpperBounds(*Const.histogramBuckets())
            .register(Kolbasa.prometheusConfig.registry)
    }
}
