package kolbasa.utils

import java.time.Duration

internal data class Execution(val startTimeEpochMillis: Long, val durationNanos: Long)
internal data class MeasureResult<T>(val execution: Execution, val result: T)

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
        // interval 'N millisecond' ==> N must be between Int.MIN_VALUE and Int.MAX_VALUE
        // PostgreSQL 15+ works well with long values, so when the least supported PG version
        // will be 15 or higher this function can be removed completely
        // The unit is chosen by the length, so that a negative duration (the mutator allows them) fits too
        val length = duration.abs()
        return when {
            length <= MAX_MILLIS -> {
                "interval '${duration.toMillis()} millisecond'"
            }

            length <= MAX_SECONDS -> {
                "interval '${duration.toSeconds()} second'"
            }

            length <= MAX_MINUTES -> {
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
