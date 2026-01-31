package kolbasa.consumer.sweep

import kolbasa.Kolbasa
import kolbasa.consumer.ConsumerSchemaHelpers
import kolbasa.utils.JdbcHelpers.useStatement
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.TimeHelper
import java.sql.Connection
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.max

object SweepHelper {

    fun needSweep(): Boolean {
        val sweepConfig = Kolbasa.sweepConfig

        // Sweep is disabled at all, stop all other checks
        if (!sweepConfig.enabled) {
            return false
        }

        // Check
        if (!checkProbability(sweepConfig.probability)) {
            return false
        }

        return true
    }

    /**
     * Run sweep for a particular queue
     *
     * Sweep tries to remove the maximum value from [SweepConfig.maxMessages] or `limit`, whichever is greater.
     *
     * @return how many expired messages were removed
     */
    fun sweep(connection: Connection, queue: Queue<*>, limit: Int): Int {
        return sweep(connection, queue, NodeId.EMPTY_NODE_ID, limit)
    }

    internal fun sweep(connection: Connection, queue: Queue<*>, nodeId: NodeId, limit: Int): Int {
        val sweepConfig = Kolbasa.sweepConfig

        // Calculate how much messages to sweep
        val messagesToSweep = max(limit, sweepConfig.maxMessages)

        return rawSweep(connection, queue, nodeId, messagesToSweep)
    }

    internal fun checkProbability(probability: Double): Boolean = when (probability) {
        0.0 -> false
        1.0 -> true
        else -> (ThreadLocalRandom.current().nextDouble() <= probability)
    }

    /**
     * Just run sweep without any checks and locks
     */
    private fun rawSweep(connection: Connection, queue: Queue<*>, nodeId: NodeId, maxMessages: Int): Int {
        val deleteQuery = ConsumerSchemaHelpers.generateDeleteExpiredMessagesQuery(queue, maxMessages)

        val (execution, removedMessages) = TimeHelper.measure {
            connection.useStatement { statement ->
                statement.executeUpdate(deleteQuery)
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.SWEEP, deleteQuery, execution, removedMessages)

        // Prometheus
        queue.queueMetrics.sweepMetrics(
            nodeId = nodeId,
            removedMessages = removedMessages,
            executionNanos = execution.durationNanos
        )

        return removedMessages
    }
}
