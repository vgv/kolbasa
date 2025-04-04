package kolbasa.consumer.sweep

import kolbasa.Kolbasa
import kolbasa.consumer.ConsumerSchemaHelpers
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.utils.Lock
import kolbasa.queue.Queue
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.TimeHelper
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

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
     * A total of SweepConfig.maxIterations will be made. Every iteration will try to remove the
     * maximum value from SweepConfig.maxRows or limit, whichever is greater.
     *
     * @return how many expired messages were removed or -1 if sweep didn't run due to
     * concurrent sweep for the queue at the same time by another consumer
     */
    fun sweep(connection: Connection, queue: Queue<*, *>, limit: Int): Int {
        val sweepConfig = Kolbasa.sweepConfig

        // Choose max rows value to sweep
        val rowsToSweep = max(limit, sweepConfig.maxRows)

        val removedRows = Lock.tryRunExclusive(queue.name) {
            rawSweep(connection, queue, rowsToSweep, sweepConfig.maxIterations)
        }

        return removedRows ?: -1
    }

    internal fun checkPeriod(queue: Queue<*, *>, period: Int): Boolean {
        // If we have to launch sweep at every consume, there is no reason to do any calculations
        if (period == SweepConfig.EVERYTIME_SWEEP_PERIOD) {
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
        var iterations = 0

        // loop while we have rows to delete or iteration < maxIterations
        val (execution, _) = TimeHelper.measure {
            do {
                val removedRows = rawSweepOneIteration(connection, queue, maxRows)
                totalRows += removedRows
                iterations++
            } while (iterations < maxIterations && removedRows == maxRows)
        }

        // Prometheus
        queue.queueMetrics.sweepMetrics(iterations, totalRows, execution.durationNanos)

        return totalRows
    }

    private fun rawSweepOneIteration(connection: Connection, queue: Queue<*, *>, maxRows: Int): Int {
        val deleteQuery = ConsumerSchemaHelpers.generateDeleteExpiredMessagesQuery(queue, maxRows)

        val (execution, removedRows) = TimeHelper.measure {
            connection.useStatement { statement ->
                statement.executeUpdate(deleteQuery)
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.SWEEP, deleteQuery, execution, removedRows)

        return removedRows
    }

    // Queue name => Sweep period counter
    private val iterationsCounter = ConcurrentHashMap<String, AtomicInteger>()

}
