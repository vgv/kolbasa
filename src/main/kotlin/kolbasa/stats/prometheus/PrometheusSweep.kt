package kolbasa.stats.prometheus

import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram
import kolbasa.Kolbasa

internal object PrometheusSweep {

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

    val sweepOneIterationRowsRemovedCounter: Counter = Counter.builder()
        .name("kolbasa_sweep_iteration_removed_rows")
        .help("Number of rows removed by one sweep iteration")
        .labelNames("queue")
        .register(Kolbasa.prometheusConfig.registry)

    val sweepOneIterationDuration: Histogram = Histogram.builder()
        .name("kolbasa_sweep_iteration_duration_seconds")
        .help("One sweep iteration duration")
        .labelNames("queue")
        .classicOnly()
        .classicUpperBounds(*Const.histogramBuckets())
        .register(Kolbasa.prometheusConfig.registry)
}
