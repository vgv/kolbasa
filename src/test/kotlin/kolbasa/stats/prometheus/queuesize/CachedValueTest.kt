package kolbasa.stats.prometheus.queuesize

import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertFalse
import kotlin.test.assertTrue

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
