package kolbasa.utils

import java.time.Duration
import java.time.LocalDateTime

internal object TimeHelper {

    data class Execution(val startTime: LocalDateTime, val duration: Duration, val affectedRows: Int)

    fun measure(block: () -> Int): Execution {
        val startTime = LocalDateTime.now()
        val executionStartNanos = System.nanoTime()
        val affectedRows = block()

        return Execution(startTime, Duration.ofNanos(System.nanoTime() - executionStartNanos), affectedRows)
    }

}
