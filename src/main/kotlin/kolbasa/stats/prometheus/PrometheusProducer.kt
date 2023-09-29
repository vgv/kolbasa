package kolbasa.stats.prometheus

import io.prometheus.client.Counter
import io.prometheus.client.Histogram

internal object PrometheusProducer {

    val producerSendCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("producer_send")
        .help("Amount of producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .create()
        .register<Counter>()

    val producerSendRowsCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("producer_send_rows")
        .help("Amount of all (successful and failed) rows sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .create()
        .register<Counter>()

    val producerSendFailedRowsCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("producer_send_rows_failed")
        .help("Amount of failed rows sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .create()
        .register<Counter>()

    val producerSendDuration = Histogram.Builder()
        .namespace("kolbasa")
        .name("producer_send_duration_seconds")
        .help("Producer send() calls duration")
        .labelNames("queue", "partial_insert_type")
        .buckets(*Const.histogramBuckets())
        .create()
        .register<Histogram>()

    val producerSendBytesCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("producer_send_bytes")
        .help("Amount of bytes sent by producer send() calls")
        .labelNames("queue", "partial_insert_type")
        .create()
        .register<Counter>()

}
