package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import io.prometheus.metrics.core.datapoints.GaugeDataPoint
import java.time.Duration

internal object Extensions {

    fun CounterDataPoint.incInt(n: Int) {
        this.inc(n.toDouble())
    }

    fun CounterDataPoint.incLong(n: Long) {
        this.inc(n.toDouble())
    }

    fun GaugeDataPoint.setLong(n: Long) {
        this.set(n.toDouble())
    }

    fun DistributionDataPoint.observeNanos(nanos: Long) {
        this.observe(nanos / NANOS_IN_SECOND)
    }

    fun Duration.asSeconds(): Double {
        return this.toNanos() / NANOS_IN_SECOND
    }

    private const val NANOS_IN_SECOND = 1_000_000_000.0
}
