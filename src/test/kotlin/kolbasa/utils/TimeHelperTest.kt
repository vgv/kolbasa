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

}
