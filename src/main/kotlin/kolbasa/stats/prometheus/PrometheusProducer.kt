package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram

internal object PrometheusProducer {

    val producerSendCounter = Counter.builder()
        .name("kolbasa_producer_send")
        .help("Amount of producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

    val producerSendRowsCounter = Counter.builder()
        .name("kolbasa_producer_send_rows")
        .help("Amount of all (successful and failed) rows sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

    val producerSendFailedRowsCounter = Counter.builder()
        .name("kolbasa_producer_send_rows_failed")
        .help("Amount of failed rows sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

    val producerSendDuration = Histogram.builder()
        .name("kolbasa_producer_send_duration_seconds")
        .help("Producer send() calls duration")
        .labelNames("queue", "partial_insert_type")
        .classicOnly()
        .classicUpperBounds(*Const.histogramBuckets())
        .register()

    val producerSendBytesCounter = Counter.builder()
        .name("kolbasa_producer_send_bytes")
        .help("Amount of bytes sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .register()

}
