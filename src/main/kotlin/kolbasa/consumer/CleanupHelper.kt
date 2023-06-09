package kolbasa.consumer

import kolbasa.Kolbasa
import kolbasa.pg.Lock
import kolbasa.queue.Queue
import java.sql.Connection
import kotlin.random.Random

internal object CleanupHelper {

    fun cleanup(connection: Connection, queue: Queue<*, *>) {
        val cleanupConfig = Kolbasa.cleanupConfig
        if (cleanupConfig.cleanupProbabilityPercent < Random.nextInt(0, 100)) {
            return
        }

        val lockId = cleanupConfig.cleanupLockIdGenerator(queue)

        Lock.tryRunExclusive(connection, lockId) { _ ->
            rawCleanup(connection, queue, cleanupConfig.cleanupLimit, cleanupConfig.cleanupMaxIterations)
        }
    }

    private fun rawCleanup(connection: Connection, queue: Queue<*, *>, limit: Int, maxIterations: Int) {
        (1..maxIterations).forEach { _ ->
            val removedRows = ConsumerSchemaHelpers.deleteExpiredMessages(connection, queue, limit)
            if (removedRows == 0) {
                // if there are no rows to delete, stop the loop right now
                return
            }
        }
    }

}
