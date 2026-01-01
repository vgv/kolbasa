package kolbasa.stats.prometheus.queuesize

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit

class CachedValueTest {

    @Test
    fun testIsStillValid() {
        val cachedValue = CachedValue(123)
        val maxInterval = Duration.ofSeconds(1)

        assertTrue(cachedValue.isStillValid(maxInterval))
        // sleep a bit longer than maxInterval
        TimeUnit.MILLISECONDS.sleep(maxInterval.toMillis() + 10)
        // check again
        assertFalse(cachedValue.isStillValid(maxInterval))
    }
}
