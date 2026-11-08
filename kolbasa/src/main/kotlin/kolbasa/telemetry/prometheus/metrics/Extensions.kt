package kolbasa.telemetry.prometheus.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.TimeUnit

internal object Extensions {

    fun Counter.incInt(n: Int) {
        this.increment(n.toDouble())
    }

    fun Counter.incLong(n: Long) {
        this.increment(n.toDouble())
    }

    fun GaugeValue.setLong(n: Long) {
        this.set(n.toDouble())
    }

    fun Timer.observeNanos(nanos: Long) {
        this.record(nanos, TimeUnit.NANOSECONDS)
    }

    fun Duration.asSeconds(): Double {
        return this.toNanos() / NANOS_IN_SECOND
    }

    private const val NANOS_IN_SECOND = 1_000_000_000.0
}
