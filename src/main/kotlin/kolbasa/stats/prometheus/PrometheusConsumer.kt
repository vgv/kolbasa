package kolbasa.stats.prometheus

import io.prometheus.client.Counter
import io.prometheus.client.Histogram

internal object PrometheusConsumer {

    val consumerReceiveCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("consumer_receive")
        .help("Amount of consumer receive() calls")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val consumerReceiveBytesCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("consumer_receive_bytes")
        .help("Amount of bytes read by consumer receive() calls")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val consumerReceiveRowsCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("consumer_receive_rows")
        .help("Amount of rows received by consumer receive() calls")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val consumerReceiveDuration = Histogram.Builder()
        .namespace("kolbasa")
        .name("consumer_receive_duration_seconds")
        .help("Consumer receive() calls duration")
        .labelNames("queue")
        .buckets(*Const.histogramBuckets())
        .create()
        .register<Histogram>()


    val consumerDeleteCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("consumer_delete")
        .help("Amount of consumer delete() calls")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val consumerDeleteRowsCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("consumer_delete_rows")
        .help("Amount of rows removed by consumer delete() calls")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val consumerDeleteDuration = Histogram.Builder()
        .namespace("kolbasa")
        .name("consumer_delete_duration_seconds")
        .help("Consumer delete() calls duration")
        .labelNames("queue")
        .buckets(*Const.histogramBuckets())
        .create()
        .register<Histogram>()

}
