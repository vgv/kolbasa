package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram
import kolbasa.Kolbasa

internal object PrometheusConsumer {

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
