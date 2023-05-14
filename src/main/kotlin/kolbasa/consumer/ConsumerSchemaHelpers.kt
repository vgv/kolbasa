package kolbasa.consumer

import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.Queue
import kolbasa.queue.QueueDataType
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import kolbasa.utils.IntBox
import kolbasa.utils.LongBox
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

internal object ConsumerSchemaHelpers {

    fun <M : Any> generateSelectPreparedQuery(
        queue: Queue<*, M>,
        consumerOptions: ConsumerOptions,
        receiveOptions: ReceiveOptions<M>,
        limit: Int
    ): String {
        // Columns
        val columns = mutableListOf(
            Const.ID_COLUMN_NAME,
            Const.CREATED_AT_COLUMN_NAME,
            Const.PROCESSING_AT_COLUMN_NAME,
            Const.ATTEMPTS_COLUMN_NAME,
            Const.DATA_COLUMN_NAME
        )

        // if we need metadata - we need to read these fields
        if (receiveOptions.readMetadata) {
            queue.metadataDescription?.fields?.forEach { metaField ->
                columns += metaField.dbColumnName
            }
        }

        // Clauses
        val clauses = mutableListOf(
            "(${Const.SCHEDULED_AT_COLUMN_NAME} is null or ${Const.SCHEDULED_AT_COLUMN_NAME} <= clock_timestamp())",
            "${Const.ATTEMPTS_COLUMN_NAME}>0"
        )
        receiveOptions.filter?.let { filter ->
            clauses += filter.toSqlClause(queue)
        }

        // OrderBy
        val orderBy = mutableListOf<String>()
        receiveOptions.order?.forEach { order ->
            orderBy += order.dbOrderClause
        }
        orderBy += "${Const.SCHEDULED_AT_COLUMN_NAME} asc nulls first"
        orderBy += "${Const.CREATED_AT_COLUMN_NAME} asc"

        val visibilityTimeout = QueueHelpers.calculateVisibilityTimeout(queue.options, consumerOptions, receiveOptions)

        return """
            update ${queue.dbTableName}
            set
                ${Const.SCHEDULED_AT_COLUMN_NAME}=clock_timestamp() + interval '${visibilityTimeout.toMillis()} millisecond',
                ${Const.PROCESSING_AT_COLUMN_NAME}=clock_timestamp(),
                ${Const.ATTEMPTS_COLUMN_NAME}=${Const.ATTEMPTS_COLUMN_NAME}-1,
                consumer=?
            where id in (
                select id
                from ${queue.dbTableName}
                where
                    ${clauses.joinToString(separator = " and ")}
                order by
                    ${orderBy.joinToString(separator = ",")}
                limit $limit
                for update skip locked
            )
            returning ${columns.joinToString(separator = ",")};
        """.trimIndent()
    }

    fun <M : Any> fillSelectPreparedQuery(
        queue: Queue<*, M>,
        consumerOptions: ConsumerOptions,
        receiveOptions: ReceiveOptions<M>,
        preparedStatement: PreparedStatement
    ) {
        if (consumerOptions.consumer != null) {
            preparedStatement.setString(1, consumerOptions.consumer)
        } else {
            preparedStatement.setNull(1, Types.VARCHAR)
        }

        // fill filter clauses
        receiveOptions.filter?.fillPreparedQuery(queue, preparedStatement, IntBox(2))
    }

    fun <V, M : Any> read(
        queue: Queue<V, M>,
        receiveOptions: ReceiveOptions<M>,
        resultSet: ResultSet,
        approxBytesCounter: LongBox
    ): Message<V, M> {
        var columnIndex = 1

        val id = resultSet.getLong(columnIndex++)
        val createdAt = resultSet.getTimestamp(columnIndex++).time
        val processingAt = resultSet.getTimestamp(columnIndex++).time
        val attempts = resultSet.getInt(columnIndex++)

        val data = when (queue.dataType) {
            is QueueDataType.Json -> {
                val data = resultSet.getString(columnIndex++)
                approxBytesCounter.inc(data.length)
                queue.dataType.deserializer(data)
            }

            is QueueDataType.Binary -> {
                val data = resultSet.getBytes(columnIndex++)
                approxBytesCounter.inc(data.size)
                queue.dataType.deserializer(data)
            }

            is QueueDataType.Text -> {
                val data = resultSet.getString(columnIndex++)
                approxBytesCounter.inc(data.length)
                queue.dataType.deserializer(data)
            }

            is QueueDataType.Int -> {
                approxBytesCounter.inc(4)
                queue.dataType.deserializer(resultSet.getInt(columnIndex++))
            }

            is QueueDataType.Long -> {
                approxBytesCounter.inc(8)
                queue.dataType.deserializer(resultSet.getLong(columnIndex++))
            }
        }

        val meta = if (receiveOptions.readMetadata) {
            queue.metadataDescription?.let {
                val metaValues = Array(queue.metadataDescription.fields.size) { index ->
                    val field = queue.metadataDescription.fields[index]
                    field.readResultSet(resultSet, columnIndex++)
                }

                queue.metadataDescription.createInstance(metaValues)
            }
        } else {
            null
        }

        return Message(id, createdAt, processingAt, attempts, data, meta)
    }

    fun generateDeleteQuery(queue: Queue<*, *>, id: Long): String {
        return "delete from ${queue.dbTableName} where ${Const.ID_COLUMN_NAME}=$id"
    }

    fun generateDeleteQuery(queue: Queue<*, *>, ids: List<Long>): String {
        check(ids.isNotEmpty())

        val idsList = ids.joinToString(separator = ",", prefix = "(", postfix = ")")
        return "delete from ${queue.dbTableName} where ${Const.ID_COLUMN_NAME} in $idsList"
    }

    fun deleteExpiredMessages(connection: Connection, queue: Queue<*, *>, limit: Int): Int {
        val sql = """
            delete from
                ${queue.dbTableName}
            where id in (
                select id
                from ${queue.dbTableName}
                where
                    ${Const.ATTEMPTS_COLUMN_NAME} <= 0 and
                    ${Const.SCHEDULED_AT_COLUMN_NAME} <= clock_timestamp()
                limit $limit
            )
    """.trimIndent()

        return connection.useStatement { statement ->
            statement.executeUpdate(sql)
        }
    }


}
