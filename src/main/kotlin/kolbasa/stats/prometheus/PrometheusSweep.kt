package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram

internal object PrometheusSweep {

    val sweepCounter = Counter.builder()
        .name("kolbasa_sweep")
        .help("Amount of sweeps")
        .labelNames("queue")
        .register()

    val sweepIterationsCounter = Counter.builder()
        .name("kolbasa_sweep_iterations")
        .help("Sweep iterations (every sweep can have multiple iterations)")
        .labelNames("queue")
        .register()

    val sweepRowsRemovedCounter = Counter.builder()
        .name("kolbasa_sweep_removed_rows")
        .help("Amount of rows removed by sweep")
        .labelNames("queue")
        .register()

    val sweepDuration = Histogram.builder()
        .name("kolbasa_sweep_duration_seconds")
        .help("Sweep duration")
        .labelNames("queue")
        .classicOnly()
        .classicUpperBounds(*Const.histogramBuckets())
        .register()
}
