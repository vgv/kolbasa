package kolbasa.consumer

import kolbasa.Kolbasa
import kolbasa.pg.Lock
import kolbasa.queue.Queue
import java.sql.Connection
import kotlin.random.Random

internal object SweepHelper {

    fun sweep(connection: Connection, queue: Queue<*, *>) {
        val sweepConfig = Kolbasa.sweepConfig

        if (!sweepConfig.enabled) {
            return
        }

        if (probability(sweepConfig.cleanupProbabilityPercent)) {
            return
        }

        val lockId = sweepConfig.lockIdGenerator(queue)

        Lock.tryRunExclusive(connection, lockId) { _ ->
            rawSweep(connection, queue, sweepConfig.maxRows, sweepConfig.maxIterations)
        }
    }

    fun probability(probabilityPercent: Int): Boolean {
        return probabilityPercent > Random.nextInt(0, 100)
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

}
