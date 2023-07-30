package kolbasa.consumer

import kolbasa.Kolbasa
import kolbasa.pg.Lock
import kolbasa.queue.Queue
import kolbasa.schema.Const
import java.sql.Connection
import java.util.concurrent.atomic.AtomicLong

internal object SweepHelper {

    fun sweep(connection: Connection, queue: Queue<*, *>) {
        val sweepConfig = Kolbasa.sweepConfig

        // Sweep is disabled at all, stop all other checks
        if (!sweepConfig.enabled) {
            return
        }

        // Check
        if (!checkPeriod(sweepConfig.period)) {
            return
        }

        val lockId = sweepConfig.lockIdGenerator(queue)

        Lock.tryRunExclusive(connection, lockId) { _ ->
            rawSweep(connection, queue, sweepConfig.maxRows, sweepConfig.maxIterations)
        }
    }

    fun checkPeriod(period: Int): Boolean {
        // If we have to launch sweep at every consume, there is no reason to do any calculations
        if (period == Const.EVERYTIME_SWEEP_PERIOD) {
            return true
        }

        return (iterationsCounter.incrementAndGet() % period) == 0L
    }

    private fun rawSweep(connection: Connection, queue: Queue<*, *>, maxRows: Int, maxIterations: Int) {
        (1..maxIterations).forEach { _ ->
            val removedRows = ConsumerSchemaHelpers.deleteExpiredMessages(connection, queue, maxRows)
            if (removedRows == 0) {
                // if there are no rows to delete, stop the loop right now
                return
            }
        }
    }

    private val iterationsCounter = AtomicLong(0)

}
