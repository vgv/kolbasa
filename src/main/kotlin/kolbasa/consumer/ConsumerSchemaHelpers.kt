package kolbasa.consumer

import kolbasa.consumer.filter.ColumnIndex
import kolbasa.queue.Queue
import kolbasa.queue.QueueDataType
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import kolbasa.utils.LongBox
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
        val dataColumns = mutableListOf(
            Const.ID_COLUMN_NAME,
            Const.CREATED_AT_COLUMN_NAME,
            Const.PROCESSING_AT_COLUMN_NAME,
            Const.REMAINING_ATTEMPTS_COLUMN_NAME,
            Const.DATA_COLUMN_NAME
        )
        // if we need metadata - we need to read these fields
        if (receiveOptions.readMetadata) {
            queue.metadataDescription?.fields?.forEach { metaField ->
                dataColumns += metaField.dbColumnName
            }
        }

        // ----------------------------------------------------------

        // 'where' clauses
        val whereClauses = mutableListOf(
            "${Const.SCHEDULED_AT_COLUMN_NAME} <= clock_timestamp()",
            "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}>0"
        )
        receiveOptions.filter?.let { filter ->
            whereClauses += filter.toSqlClause(queue)
        }

        // ----------------------------------------------------------
        val sortColumns = mutableListOf(
            Const.ID_COLUMN_NAME,
            Const.SCHEDULED_AT_COLUMN_NAME
        )

        // 'order by' clauses
        val orderByClauses = mutableListOf<String>()
        // custom ordering clauses first, if any
        receiveOptions.order?.forEach { order ->
            orderByClauses += order.dbOrderClause
            sortColumns += order.dbColumnName
        }
        // after custom clauses – standard
        orderByClauses += Const.SCHEDULED_AT_COLUMN_NAME
        orderByClauses += Const.CREATED_AT_COLUMN_NAME

        // ----------------------------------------------------------

        val visibilityTimeout = QueueHelpers.calculateVisibilityTimeout(queue.options, consumerOptions, receiveOptions)

        return """
            with
            id_to_update as (
                select ${sortColumns.joinToString(separator = ",")}
                from ${queue.dbTableName}
                where
                    ${whereClauses.joinToString(separator = " and ")}
                order by
                    ${orderByClauses.joinToString(separator = ",")}
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
                returning ${dataColumns.joinToString(separator = ",")}
            )
            select updated.* from updated
                inner join id_to_update on (updated.${Const.ID_COLUMN_NAME}=id_to_update.${Const.ID_COLUMN_NAME})
            order by ${orderByClauses.joinToString(separator = ",")}
        """.trimIndent()
    }

    fun <Meta : Any> fillSelectPreparedQuery(
        queue: Queue<*, Meta>,
        consumerOptions: ConsumerOptions,
        receiveOptions: ReceiveOptions<Meta>,
        preparedStatement: PreparedStatement
    ) {
        val columnIndex = ColumnIndex()

        // fill filter clauses, if any
        receiveOptions.filter?.fillPreparedQuery(queue, preparedStatement, columnIndex)

        // consumer name, if any
        if (consumerOptions.consumer != null) {
            preparedStatement.setString(columnIndex.nextIndex(), consumerOptions.consumer)
        } else {
            preparedStatement.setNull(columnIndex.nextIndex(), Types.VARCHAR)
        }
    }

    fun <Data, Meta : Any> read(
        queue: Queue<Data, Meta>,
        receiveOptions: ReceiveOptions<Meta>,
        resultSet: ResultSet,
        approxBytesCounter: LongBox
    ): Message<Data, Meta> {
        var columnIndex = 1

        val id = resultSet.getLong(columnIndex++)
        val createdAt = resultSet.getTimestamp(columnIndex++).time
        val processingAt = resultSet.getTimestamp(columnIndex++).time
        val attempts = resultSet.getInt(columnIndex++)

        val data = when (queue.dataType) {
            is QueueDataType.Json -> {
                val data = resultSet.getString(columnIndex++)
                // I know that str.length != bytes.size, but it's ok for now
                // I don't want to convert string to bytes just for metrics because it's not cheap
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
                // I know that str.length != bytes.size, but it's ok for now
                // I don't want to convert string to bytes just for metrics because it's not cheap
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

    fun generateDeleteQuery(queue: Queue<*, *>, ids: List<Long>): String {
        check(ids.isNotEmpty()) {
            "ID list must not be empty"
        }

        val idsList = ids.joinToString(separator = ",", prefix = "(", postfix = ")")
        return "delete from ${queue.dbTableName} where ${Const.ID_COLUMN_NAME} in $idsList"
    }

    fun generateDeleteExpiredMessagesQuery(queue: Queue<*, *>, limit: Int): String {
        return """
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
    }

}
