package kolbasa.utils

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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

        assertFailsWith<IllegalArgumentException> {
            TimeHelper.generatePostgreSQLInterval(Duration.ofMinutes(Int.MAX_VALUE.toLong() + 1))
        }
    }

}
