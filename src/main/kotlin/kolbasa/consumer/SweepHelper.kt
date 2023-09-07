package kolbasa.consumer

import kolbasa.Kolbasa
import kolbasa.pg.Lock
import kolbasa.queue.Queue
import kolbasa.schema.Const
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

object SweepHelper {

    fun needSweep(queue: Queue<*, *>): Boolean {
        val sweepConfig = Kolbasa.sweepConfig

        // Sweep is disabled at all, stop all other checks
        if (!sweepConfig.enabled) {
            return false
        }

        // Check
        if (!checkPeriod(queue, sweepConfig.period)) {
            return false
        }

        return true
    }

    /**
     * Run sweep for a particular queue
     *
     * @return how many expired messages were removed or Int.MIN_VALUE if sweep didn't run due to
     * concurrent sweep for the queue at the same time by another consumer
     */
    fun sweep(connection: Connection, queue: Queue<*, *>): Int {
        val sweepConfig = Kolbasa.sweepConfig

        val lockId = sweepConfig.lockIdGenerator(queue)

        val removedRows = Lock.tryRunExclusive(connection, lockId) { _ ->
            rawSweep(connection, queue, sweepConfig.maxRows, sweepConfig.maxIterations)
        }

        return removedRows ?: Int.MIN_VALUE
    }

    internal fun checkPeriod(queue: Queue<*, *>, period: Int): Boolean {
        // If we have to launch sweep at every consume, there is no reason to do any calculations
        if (period == Const.EVERYTIME_SWEEP_PERIOD) {
            return true
        }

        val counter = iterationsCounter.computeIfAbsent(queue.name) { _ ->
            AtomicInteger(0)
        }

        return (counter.incrementAndGet() % period) == 0
    }

    /**
     * Just run sweep without any checks and locks
     */
    private fun rawSweep(connection: Connection, queue: Queue<*, *>, maxRows: Int, maxIterations: Int): Int {
        var totalRows = 0
        var iteration = 0

        do {
            // loop while we have rows to delete or iteration < maxIterations
            val removedRows = ConsumerSchemaHelpers.deleteExpiredMessages(connection, queue, maxRows)
            totalRows += removedRows
            iteration++
        } while (iteration < maxIterations && removedRows > 0)

        return totalRows
    }

    // Queue name => Sweep period counter
    private val iterationsCounter = ConcurrentHashMap<String, AtomicInteger>()

}
