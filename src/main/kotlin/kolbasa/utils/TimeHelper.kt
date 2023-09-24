package kolbasa.utils

data class Execution(
    val startTimeEpochMillis: Long,
    val durationNanos: Long,
    val affectedRows: Int
)

data class ExecutionNanos<T>(val durationNanos: Long, val result: T)

internal object TimeHelper {

    fun <T> measureNanos(block: () -> T): ExecutionNanos<T> {
        val executionStartNanos = System.nanoTime()
        val result = block()
        val executionEndNanos = System.nanoTime()

        return ExecutionNanos(durationNanos = executionEndNanos - executionStartNanos, result = result)
    }

    fun measure(block: () -> Int): Execution {
        val startTime = System.currentTimeMillis()
        val executionStartNanos = System.nanoTime()
        val affectedRows = block()
        val executionEndNanos = System.nanoTime()

        return Execution(
            startTimeEpochMillis = startTime,
            durationNanos = executionEndNanos - executionStartNanos,
            affectedRows = affectedRows
        )
    }

}
