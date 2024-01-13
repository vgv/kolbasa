package kolbasa.stats.prometheus.queuesize

import java.time.Duration
import java.time.Instant

internal data class CachedValue(val value: Long) {

    private val timestamp = Instant.now()

    fun isStillValid(maxAllowedInterval: Duration): Boolean {
        val now = Instant.now()
        val currentInterval = Duration.between(timestamp, now)
        return currentInterval <= maxAllowedInterval
    }
}
