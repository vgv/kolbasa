package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram
import kolbasa.Kolbasa

internal object PrometheusSweepMetrics {

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
