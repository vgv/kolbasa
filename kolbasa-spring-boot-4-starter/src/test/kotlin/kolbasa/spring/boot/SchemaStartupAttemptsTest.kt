package kolbasa.spring.boot

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import java.time.Duration

class SchemaStartupAttemptsTest {

    @Test
    fun testFirstAttemptGoesThrough_NoMoreAttempts() {
        var calls = 0
        val result = SchemaStartupAttempts(5, Duration.ZERO).run({ ++calls }, { true })

        assertEquals(1, result)
        assertEquals(1, calls)
    }

    @Test
    fun testGoesThroughLater_StopsThere() {
        var calls = 0
        val result = SchemaStartupAttempts(5, Duration.ZERO).run({ ++calls }, { it == 3 })

        assertEquals(3, result)
        assertEquals(3, calls)
    }

    @Test
    fun testNeverGoesThrough_RunsOutOfAttempts() {
        var calls = 0
        val result = SchemaStartupAttempts(3, Duration.ZERO).run({ ++calls }, { false })

        // What the last attempt returned
        assertEquals(3, result)
        assertEquals(3, calls)
    }

    @Test
    fun testRetryDelay_BeforeEveryNextAttempt() {
        val start = System.nanoTime()
        SchemaStartupAttempts(3, Duration.ofMillis(100)).run({ }, { false })

        // Two pauses for three attempts
        val elapsed = Duration.ofNanos(System.nanoTime() - start)
        assertTrue(elapsed >= Duration.ofMillis(200)) { "Elapsed: $elapsed" }
    }

    @Test
    fun testRead_Defaults() {
        val startup = SchemaStartupAttempts.read(environment(), PREFIX)

        assertEquals(5, startup.attempts)
        assertEquals(Duration.ofSeconds(1), startup.retryDelay)
    }

    @Test
    fun testRead_Values() {
        val environment = environment("$PREFIX.startup-attempts" to "3", "$PREFIX.startup-retry-delay" to "250ms")
        val startup = SchemaStartupAttempts.read(environment, PREFIX)

        assertEquals(3, startup.attempts)
        assertEquals(Duration.ofMillis(250), startup.retryDelay)
    }

    @Test
    fun testRead_NoAttempts_Fails() {
        val error = assertThrows<IllegalArgumentException> {
            SchemaStartupAttempts.read(environment("$PREFIX.startup-attempts" to "0"), PREFIX)
        }
        assertTrue("$PREFIX.startup-attempts" in error.message.orEmpty()) { "Unexpected message: ${error.message}" }
    }

    @Test
    fun testRead_NegativeDelay_Fails() {
        val error = assertThrows<IllegalArgumentException> {
            SchemaStartupAttempts.read(environment("$PREFIX.startup-retry-delay" to "-1s"), PREFIX)
        }
        assertTrue("$PREFIX.startup-retry-delay" in error.message.orEmpty()) { "Unexpected message: ${error.message}" }
    }

    private fun environment(vararg properties: Pair<String, String>): StandardEnvironment {
        return StandardEnvironment().apply {
            propertySources.addFirst(MapPropertySource("test", mapOf(*properties)))
        }
    }

    private companion object {
        const val PREFIX = "kolbasa.test.schema"
    }
}
