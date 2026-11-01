package kolbasa.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class TimeHelperTest {

    @Test
    fun testGeneratePostgreSQLInterval() {
        assertEquals(
            TimeHelper.generatePostgreSQLInterval(Duration.ofMillis(2_000_000_000)),
            "interval '2000000000 millisecond'"
        )

        assertEquals(
            TimeHelper.generatePostgreSQLInterval(Duration.ofSeconds(2_000_000_000)),
            "interval '2000000000 second'"
        )

        assertEquals(
            TimeHelper.generatePostgreSQLInterval(Duration.ofMinutes(2_000_000_000)),
            "interval '2000000000 minute'"
        )

        assertThrows<IllegalArgumentException> {
            TimeHelper.generatePostgreSQLInterval(Duration.ofMinutes(Int.MAX_VALUE.toLong() + 1))
        }
    }

    /**
     * A negative duration gets the same unit as the positive one of the same length.
     *
     * The unit was chosen by comparing the duration with the upper limit only, and every negative duration is below it.
     * So all of them went in milliseconds, and below -24.8 days (`Int.MIN_VALUE` ms) PostgreSQL 10–14 failed with
     * "interval field value out of range". The mutator allows a negative duration, for example
     * `AddScheduledAt(Duration.ofDays(-30))`.
     *
     * Expected: every unit takes negative values down to `-Int.MAX_VALUE`, -30 days go in seconds, and a duration
     * longer than the minutes can hold fails, as a positive one does.
     */
    @Test
    fun testGeneratePostgreSQLInterval_Negative() {
        assertEquals(
            "interval '-2000000000 millisecond'",
            TimeHelper.generatePostgreSQLInterval(Duration.ofMillis(-2_000_000_000))
        )

        assertEquals(
            "interval '-2592000 second'",
            TimeHelper.generatePostgreSQLInterval(Duration.ofDays(-30))
        )

        assertEquals(
            "interval '-2000000000 second'",
            TimeHelper.generatePostgreSQLInterval(Duration.ofSeconds(-2_000_000_000))
        )

        assertEquals(
            "interval '-2000000000 minute'",
            TimeHelper.generatePostgreSQLInterval(Duration.ofMinutes(-2_000_000_000))
        )

        assertThrows<IllegalArgumentException> {
            TimeHelper.generatePostgreSQLInterval(Duration.ofMinutes(-(Int.MAX_VALUE.toLong() + 1)))
        }
    }

}
