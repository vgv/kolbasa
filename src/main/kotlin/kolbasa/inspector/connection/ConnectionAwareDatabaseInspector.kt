package kolbasa.inspector.connection

import kolbasa.inspector.*
import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.ColumnIndex
import kolbasa.utils.JdbcHelpers.readBoolean
import kolbasa.utils.JdbcHelpers.readLong
import kolbasa.utils.JdbcHelpers.usePreparedStatement
import kolbasa.utils.JdbcHelpers.useStatement
import java.sql.Connection
import java.time.Duration

/**
 * Default implementation of [ConnectionAwareInspector]
 */
class ConnectionAwareDatabaseInspector : ConnectionAwareInspector {

    override fun count(
        connection: Connection,
        queue: Queue<*>,
        options: CountOptions
    ): Messages {
        val (query, samplePercent) = InspectorSchemaHelpers.generateCountWithFilterQuery(connection, queue, options)

        return connection.usePreparedStatement(query) { ps ->
            // Fill prepared statement parameters for the filter condition, if it exists
            options.filter?.let { condition ->
                val columnIndex = ColumnIndex()
                condition.fillPreparedQuery(ps, columnIndex)
            }

            ps.executeQuery().use { rs ->
                rs.next()
                Messages(
                    scheduled = rs.getLong(1).normalize(samplePercent),
                    ready = rs.getLong(2).normalize(samplePercent),
                    inFlight = rs.getLong(3).normalize(samplePercent),
                    retry = rs.getLong(4).normalize(samplePercent),
                    dead = rs.getLong(5).normalize(samplePercent),
                )
            }
        }
    }


    override fun <V> distinctValues(
        connection: Connection,
        queue: Queue<*>,
        metaField: MetaField<V>,
        limit: Int,
        options: DistinctValuesOptions
    ): List<V?> {
        val (query, _) = InspectorSchemaHelpers.generateDistinctValuesQuery(connection, queue, metaField, limit, options)

        val result = ArrayList<V?>(limit)
        connection.usePreparedStatement(query) { ps ->
            // Fill prepared statement parameters for the filter condition, if exists
            options.filter?.let { condition ->
                val columnIndex = ColumnIndex()
                condition.fillPreparedQuery(ps, columnIndex)
            }

            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    val value = metaField.readValue(rs, 1)
                    result += value?.value
                }
            }
        }
        return result
    }

    override fun size(connection: Connection, queue: Queue<*>): Long {
        val query = InspectorSchemaHelpers.generateSizeQuery(queue)
        return connection.readLong(query)
    }

    override fun isEmpty(connection: Connection, queue: Queue<*>): Boolean {
        val query = InspectorSchemaHelpers.generateIsEmptyQuery(queue)
        return connection.readBoolean(query)
    }

    override fun isDeadOrEmpty(connection: Connection, queue: Queue<*>): Boolean {
        val query = InspectorSchemaHelpers.generateIsDeadOrEmptyQuery(queue)
        return connection.readBoolean(query)
    }

    override fun messageAge(connection: Connection, queue: Queue<*>): MessageAge {
        val query = InspectorSchemaHelpers.generateMessageAgeQuery(queue)
        return connection.useStatement(query) { rs ->
            rs.next()

            val oldestSeconds = rs.getDouble(1)
            val oldest = if (rs.wasNull()) null else Duration.ofMillis((oldestSeconds * 1000).toLong())

            val newestSeconds = rs.getDouble(2)
            val newest = if (rs.wasNull()) null else Duration.ofMillis((newestSeconds * 1000).toLong())

            val oldestReadySeconds = rs.getDouble(3)
            val oldestReady = if (rs.wasNull()) null else Duration.ofMillis((oldestReadySeconds * 1000).toLong())

            MessageAge(oldest, newest, oldestReady)
        }
    }

    private fun Long.normalize(samplePercent: Float): Long {
        return (this * 100 / samplePercent).toLong()
    }
}
