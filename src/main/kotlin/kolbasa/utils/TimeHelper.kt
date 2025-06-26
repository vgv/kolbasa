package kolbasa.utils

import java.time.Duration

data class Execution(val startTimeEpochMillis: Long, val durationNanos: Long)
data class MeasureResult<T>(val execution: Execution, val result: T)

internal object TimeHelper {

    fun <T> measure(block: () -> T): MeasureResult<T> {
        val startTime = System.currentTimeMillis()

        val executionStartNanos = System.nanoTime()
        val result = block()
        val execution = Execution(startTime, System.nanoTime() - executionStartNanos)

        return MeasureResult(execution, result)
    }

    fun generatePostgreSQLInterval(duration: Duration): String {
        // PostgreSQL versions 10, 11, 12, 13 and 14 support only int values in 'interval' expressions
        // interval 'N millisecond' ==> N must be less than or equal to Int.MAX_VALUE
        // PostgreSQL 15+ works well with long values, so when the least supported PG version
        // will be 15 or higher this function can be removed completely
        return when {
            duration <= MAX_MILLIS -> {
                "interval '${duration.toMillis()} millisecond'"
            }

            duration <= MAX_SECONDS -> {
                "interval '${duration.toSeconds()} second'"
            }

            duration <= MAX_MINUTES -> {
                "interval '${duration.toMinutes()} minute'"
            }

            else -> {
                // "Minutes" branch above allows Kolbasa to delay messages for over 4000 years
                // When humanity invents immortality, this code will need to be rewritten, but, most likely,
                // by that time Kolbasa will stop supporting old PostgreSQL versions
                throw IllegalArgumentException("Duration is too big, not supported yet: $duration")
            }
        }
    }

    private val MAX_MILLIS = Duration.ofMillis(Int.MAX_VALUE.toLong())
    private val MAX_SECONDS = Duration.ofSeconds(Int.MAX_VALUE.toLong())
    private val MAX_MINUTES = Duration.ofMinutes(Int.MAX_VALUE.toLong())
}
