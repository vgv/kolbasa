package kolbasa.utils

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

}
