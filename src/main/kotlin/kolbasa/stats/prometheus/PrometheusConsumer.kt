package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram

internal object PrometheusConsumer {

    val consumerReceiveCounter = Counter.builder()
        .name("kolbasa_consumer_receive")
        .help("Amount of consumer receive() calls")
        .labelNames("queue")
        .register()

    val consumerReceiveBytesCounter = Counter.builder()
        .name("kolbasa_consumer_receive_bytes")
        .help("Amount of bytes read by consumer receive() calls")
        .labelNames("queue")
        .register()

    val consumerReceiveRowsCounter = Counter.builder()
        .name("kolbasa_consumer_receive_rows")
        .help("Amount of rows received by consumer receive() calls")
        .labelNames("queue")
        .register()

    val consumerReceiveDuration = Histogram.builder()
        .name("kolbasa_consumer_receive_duration_seconds")
        .help("Consumer receive() calls duration")
        .labelNames("queue")
        .classicOnly()
        .classicUpperBounds(*Const.histogramBuckets())
        .register()


    val consumerDeleteCounter = Counter.builder()
        .name("kolbasa_consumer_delete")
        .help("Amount of consumer delete() calls")
        .labelNames("queue")
        .register()

    val consumerDeleteRowsCounter = Counter.builder()
        .name("kolbasa_consumer_delete_rows")
        .help("Amount of rows removed by consumer delete() calls")
        .labelNames("queue")
        .register()

    val consumerDeleteDuration = Histogram.builder()
        .name("kolbasa_consumer_delete_duration_seconds")
        .help("Consumer delete() calls duration")
        .labelNames("queue")
        .classicOnly()
        .classicUpperBounds(*Const.histogramBuckets())
        .register()

}
