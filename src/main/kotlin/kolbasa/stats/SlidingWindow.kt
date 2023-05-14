package kolbasa.stats

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

internal class SlidingWindow(private val measure: Measure) {

    private val ticks = AtomicReference(ConcurrentHashMap<Long, Long>())

    fun inc(delta: Long) {
        val currentTick = measure.currentTick()
        ticks.get().compute(currentTick) { _, currentValue ->
            if (currentValue == null) {
                delta
            } else {
                currentValue + delta
            }
        }
    }

    fun dumpAndReset(): MeasureDump {
        val oldestValidTick = measure.oldestValidTick()
        val data = ticks.getAndSet(ConcurrentHashMap<Long, Long>())

        return MeasureDump(measure, oldestValidTick, data)
    }

}
