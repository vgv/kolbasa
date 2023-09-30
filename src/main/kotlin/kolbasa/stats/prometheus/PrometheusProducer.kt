package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram
import kolbasa.Kolbasa

internal object PrometheusProducer {

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
