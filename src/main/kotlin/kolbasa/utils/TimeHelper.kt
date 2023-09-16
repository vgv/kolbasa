package kolbasa.utils

import kolbasa.producer.MessageResult
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.concurrent.TimeUnit

data class Execution<T>(
    val startTimeEpochMillis: Long,
    val durationNanos: Long,
    val result: T
) {

    fun durationMillis() = TimeUnit.NANOSECONDS.toMillis(durationNanos)

    fun durationSeconds(): Double = durationNanos / 1_000_000_000.0

    fun startTime(): LocalDateTime {
        val instant = Instant.ofEpochMilli(startTimeEpochMillis)
        val zone = ZoneId.systemDefault()
        return LocalDateTime.ofInstant(instant, zone)
    }
}

internal object TimeHelper {

    fun <T> measure(block: () -> T): Execution<T> {
        val startTime = System.currentTimeMillis()
        val executionStartNanos = System.nanoTime()
        val result = block()
        val executionEndNanos = System.nanoTime()

        return Execution(
            startTimeEpochMillis = startTime,
            durationNanos = executionEndNanos - executionStartNanos,
            result = result
        )
    }

}
