package kolbasa.stats.prometheus

import io.prometheus.client.Counter
import io.prometheus.client.Histogram

internal object PrometheusSweep {

    val sweepCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("sweep")
        .help("Amount of sweeps")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val sweepIterationsCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("sweep_iterations")
        .help("Sweep iterations (every sweep can have multiple iterations)")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val sweepRowsRemovedCounter = Counter.Builder()
        .namespace("kolbasa")
        .name("sweep_removed_rows")
        .help("Amount of rows removed by sweep")
        .labelNames("queue")
        .create()
        .register<Counter>()

    val sweepDuration = Histogram.Builder()
        .namespace("kolbasa")
        .name("sweep_duration_seconds")
        .help("Sweep duration")
        .labelNames("queue")
        .buckets(*Const.histogramBuckets())
        .create()
        .register<Histogram>()
}
