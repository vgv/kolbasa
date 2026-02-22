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

            ps.executeQuery().use { resultSet ->
                require(resultSet.next()) {
                    "Query didn't return any results, it shouldn't happen. Query: '$query'"
                }

                Messages(
                    scheduled = resultSet.getLong(1).normalize(samplePercent),
                    ready = resultSet.getLong(2).normalize(samplePercent),
                    inFlight = resultSet.getLong(3).normalize(samplePercent),
                    retry = resultSet.getLong(4).normalize(samplePercent),
                    dead = resultSet.getLong(5).normalize(samplePercent),
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
    ): Map<V?, Long> {
        val (query, samplePercent) = InspectorSchemaHelpers.generateDistinctValuesQuery(
            connection,
            queue,
            metaField,
            limit,
            options
        )

        val result = LinkedHashMap<V?, Long>(limit)
        connection.usePreparedStatement(query) { ps ->
            // Fill prepared statement parameters for the filter condition, if exists
            options.filter?.let { condition ->
                val columnIndex = ColumnIndex()
                condition.fillPreparedQuery(ps, columnIndex)
            }

            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    val value = metaField.readValue(rs, 1)
                    val count = rs.getLong(2).normalize(samplePercent)
                    result[value?.value] = count
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
        return connection.useStatement(query) { resultSet ->
            require(resultSet.next()) {
                "Query didn't return any results, it shouldn't happen. Query: '$query'"
            }

            val oldestSeconds = resultSet.getDouble(1)
            val oldest = if (resultSet.wasNull()) null else Duration.ofMillis((oldestSeconds * 1000).toLong())

            val newestSeconds = resultSet.getDouble(2)
            val newest = if (resultSet.wasNull()) null else Duration.ofMillis((newestSeconds * 1000).toLong())

            val oldestReadySeconds = resultSet.getDouble(3)
            val oldestReady = if (resultSet.wasNull()) null else Duration.ofMillis((oldestReadySeconds * 1000).toLong())

            MessageAge(oldest, newest, oldestReady)
        }
    }

    private fun Long.normalize(samplePercent: Float): Long {
        return (this * 100 / samplePercent).toLong()
    }
}
