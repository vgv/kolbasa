package kolbasa.stats.prometheus

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint

internal object Extensions {

    fun CounterDataPoint.incInt(n: Int) {
        this.inc(n.toDouble())
    }

    fun CounterDataPoint.incLong(n: Long) {
        this.inc(n.toDouble())
    }

    fun DistributionDataPoint.observeNanos(nanos: Long) {
        this.observe(nanos / NANOS_IN_SECOND)
    }

    private const val NANOS_IN_SECOND = 1_000_000_000.0
}
