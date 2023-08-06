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

    fun <Meta : Any> generateSelectPreparedQuery(
        queue: Queue<*, Meta>,
        consumerOptions: ConsumerOptions,
        receiveOptions: ReceiveOptions<Meta>,
        limit: Int
    ): String {
        // Columns to read from database
        val columns = mutableListOf(
            Const.ID_COLUMN_NAME,
            Const.CREATED_AT_COLUMN_NAME,
            Const.PROCESSING_AT_COLUMN_NAME,
            Const.REMAINING_ATTEMPTS_COLUMN_NAME,
            Const.DATA_COLUMN_NAME
        )

        // if we need metadata - we need to read these fields
        if (receiveOptions.readMetadata) {
            queue.metadataDescription?.fields?.forEach { metaField ->
                columns += metaField.dbColumnName
            }
        }

        // 'where' clauses
        val clauses = mutableListOf(
            "(${Const.SCHEDULED_AT_COLUMN_NAME} is null or ${Const.SCHEDULED_AT_COLUMN_NAME} <= clock_timestamp())",
            "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}>0"
        )
        receiveOptions.filter?.let { filter ->
            clauses += filter.toSqlClause(queue)
        }

        // 'order by' clauses
        val orderBy = mutableListOf<String>()
        // custom ordering clauses first, if any
        receiveOptions.order?.forEach { order ->
            orderBy += order.dbOrderClause
        }
        // after custom clauses – standard
        orderBy += "${Const.SCHEDULED_AT_COLUMN_NAME} asc nulls first"
        orderBy += "${Const.CREATED_AT_COLUMN_NAME} asc"

        val visibilityTimeout = QueueHelpers.calculateVisibilityTimeout(queue.options, consumerOptions, receiveOptions)

        return """
            with
            id_to_update as (
                select ${Const.ID_COLUMN_NAME},${Const.SCHEDULED_AT_COLUMN_NAME}
                from ${queue.dbTableName}
                where
                    ${clauses.joinToString(separator = " and ")}
                order by
                    ${orderBy.joinToString(separator = ",")}
                limit $limit
                for update skip locked
            ),
            updated as (
                update ${queue.dbTableName}
                set
                    ${Const.SCHEDULED_AT_COLUMN_NAME}=clock_timestamp() + interval '${visibilityTimeout.toMillis()} millisecond',
                    ${Const.PROCESSING_AT_COLUMN_NAME}=clock_timestamp(),
                    ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}=${Const.REMAINING_ATTEMPTS_COLUMN_NAME}-1,
                    ${Const.CONSUMER_COLUMN_NAME}=?
                where ${Const.ID_COLUMN_NAME} in (select ${Const.ID_COLUMN_NAME} from id_to_update)
                returning ${columns.joinToString(separator = ",")}
            )
            select updated.* from updated
                inner join id_to_update on (updated.${Const.ID_COLUMN_NAME}=id_to_update.${Const.ID_COLUMN_NAME})
            order by ${orderBy.joinToString(separator = ",")}
        """.trimIndent()
    }

    fun <Meta : Any> fillSelectPreparedQuery(
        queue: Queue<*, Meta>,
        consumerOptions: ConsumerOptions,
        receiveOptions: ReceiveOptions<Meta>,
        preparedStatement: PreparedStatement
    ) {
        val columnIndex = IntBox(1)

        // fill filter clauses, if any
        receiveOptions.filter?.fillPreparedQuery(queue, preparedStatement, columnIndex)

        // consumer name, if any
        if (consumerOptions.consumer != null) {
            preparedStatement.setString(columnIndex.getAndIncrement(), consumerOptions.consumer)
        } else {
            preparedStatement.setNull(columnIndex.getAndIncrement(), Types.VARCHAR)
        }
    }

    fun <V, Meta : Any> read(
        queue: Queue<V, Meta>,
        receiveOptions: ReceiveOptions<Meta>,
        resultSet: ResultSet,
        approxBytesCounter: LongBox
    ): Message<V, Meta> {
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

        val meta = if (receiveOptions.readMetadata && queue.metadataDescription != null) {
            var atLeastOneValueIsNotNull = false

            val metaValues = Array(queue.metadataDescription.fields.size) { index ->
                val field = queue.metadataDescription.fields[index]
                val fieldValue = field.readResultSet(resultSet, columnIndex++)
                if (fieldValue != null) atLeastOneValueIsNotNull = true
                return@Array fieldValue
            }

            if (atLeastOneValueIsNotNull)
                queue.metadataDescription.createInstance(metaValues)
            else
                null
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
                    ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} <= 0 and
                    ${Const.SCHEDULED_AT_COLUMN_NAME} <= clock_timestamp()
                limit $limit
            )
    """.trimIndent()

        return connection.useStatement { statement ->
            statement.executeUpdate(sql)
        }
    }


}
