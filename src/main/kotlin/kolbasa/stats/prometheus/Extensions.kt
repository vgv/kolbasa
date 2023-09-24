package kolbasa.stats.prometheus

import io.prometheus.client.Counter
import io.prometheus.client.Histogram

internal object Extensions {

    fun Counter.Child.incInt(n: Int) {
        this.inc(n.toDouble())
    }

    fun Counter.Child.incLong(n: Long) {
        this.inc(n.toDouble())
    }
    fun Histogram.Child.observeNanos(nanos: Long) {
        this.observe(nanos / NANOS_IN_SECOND)
    }

    private const val NANOS_IN_SECOND = 1_000_000_000.0
}
