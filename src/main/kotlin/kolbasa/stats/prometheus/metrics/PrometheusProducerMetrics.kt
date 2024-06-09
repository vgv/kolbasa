package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Gauge
import io.prometheus.metrics.core.metrics.Histogram
import kolbasa.Kolbasa

internal object PrometheusProducerMetrics {

    val producerSendCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send")
        .help("Amount of producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

    val producerSendRowsCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send_rows")
        .help("Amount of all (successful and failed) rows sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

    val producerSendFailedRowsCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send_rows_failed")
        .help("Amount of failed rows sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

    val producerSendDuration: Histogram = Histogram.builder()
        .name("kolbasa_producer_send_duration_seconds")
        .help("Producer send() calls duration")
        .labelNames("queue", "partial_insert_type")
        .classicOnly()
        .classicUpperBounds(*Const.histogramBuckets())
        .register()

    val producerSendBytesCounter: Counter = Counter.builder()
        .name("kolbasa_producer_send_bytes")
        .help("Amount of bytes sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

    val producerQueueSizeGauge: Gauge = Gauge.builder()
        .name("kolbasa_producer_queue_size")
        .help("Producer queue size")
        .labelNames("queue")
        .register()

}
