package kolbasa.consumer

import kolbasa.cluster.Shards
import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.MetaValue
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import kolbasa.utils.BytesCounter
import kolbasa.utils.ColumnIndex
import kolbasa.utils.Helpers
import kolbasa.utils.TimeHelper
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

internal object ConsumerSchemaHelpers {

    fun generateSelectPreparedQuery(
        queue: Queue<*>,
        consumerOptions: ConsumerOptions,
        shards: Shards,
        receiveOptions: ReceiveOptions,
        limit: Int
    ): String {
        // Columns to read from database
        val dataColumns = mutableListOf(
            Const.ID_COLUMN_NAME,
            Const.SHARD_COLUMN_NAME,
            Const.CREATED_AT_COLUMN_NAME,
            Const.PROCESSING_AT_COLUMN_NAME,
            Const.SCHEDULED_AT_COLUMN_NAME,
            Const.REMAINING_ATTEMPTS_COLUMN_NAME,
            Const.DATA_COLUMN_NAME
        )
        // if we need metadata - we need to read these fields
        if (receiveOptions.readMetadata) {
            queue.metadata.fields.forEach { metaField ->
                dataColumns += metaField.dbColumnName
            }
        }
        // if we need OT data - read it
        if (receiveOptions.readOpenTelemetryData) {
            dataColumns += Const.OPENTELEMETRY_COLUMN_NAME
        }

        // ----------------------------------------------------------

        // 'where' clauses
        val whereClauses = mutableListOf(
            "${Const.SCHEDULED_AT_COLUMN_NAME} <= clock_timestamp()",
            "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}>0"
        )
        if (shards != Shards.ALL_SHARDS) {
            whereClauses += "${Const.SHARD_COLUMN_NAME} in (${shards.asText})"
        }
        receiveOptions.filter?.let { filter ->
            whereClauses += filter.toSqlClause()
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
            sortColumns += order.field.dbColumnName
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
                    ${Const.PROCESSING_AT_COLUMN_NAME}=clock_timestamp(),
                    ${Const.SCHEDULED_AT_COLUMN_NAME}=clock_timestamp() + ${TimeHelper.generatePostgreSQLInterval(visibilityTimeout)},
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

    fun fillSelectPreparedQuery(
        consumerOptions: ConsumerOptions,
        receiveOptions: ReceiveOptions,
        preparedStatement: PreparedStatement
    ) {
        val columnIndex = ColumnIndex()

        // fill filter clauses, if any
        receiveOptions.filter?.fillPreparedQuery(preparedStatement, columnIndex)

        // consumer name, if any
        if (consumerOptions.consumer != null) {
            preparedStatement.setString(columnIndex.nextIndex(), consumerOptions.consumer)
        } else {
            preparedStatement.setNull(columnIndex.nextIndex(), Types.VARCHAR)
        }
    }

    fun <Data> read(
        queue: Queue<Data>,
        receiveOptions: ReceiveOptions,
        resultSet: ResultSet,
        approxBytesCounter: BytesCounter
    ): Message<Data> {
        var columnIndex = 1

        val localId = resultSet.getLong(columnIndex++)
        val shard = resultSet.getInt(columnIndex++)
        val createdAt = resultSet.getTimestamp(columnIndex++).time
        val processingAt = resultSet.getTimestamp(columnIndex++).time
        val scheduledAt = resultSet.getTimestamp(columnIndex++).time
        val attempts = resultSet.getInt(columnIndex++)

        val data = when (queue.databaseDataType) {
            is DatabaseQueueDataType.Json -> {
                val data = resultSet.getString(columnIndex++)
                approxBytesCounter.addString(data)
                queue.databaseDataType.deserializer(data)
            }

            is DatabaseQueueDataType.Binary -> {
                val data = resultSet.getBytes(columnIndex++)
                approxBytesCounter.addByteArray(data)
                queue.databaseDataType.deserializer(data)
            }

            is DatabaseQueueDataType.Text -> {
                val data = resultSet.getString(columnIndex++)
                approxBytesCounter.addString(data)
                queue.databaseDataType.deserializer(data)
            }

            is DatabaseQueueDataType.Int -> {
                approxBytesCounter.addInt()
                queue.databaseDataType.deserializer(resultSet.getInt(columnIndex++))
            }

            is DatabaseQueueDataType.Long -> {
                approxBytesCounter.addLong()
                queue.databaseDataType.deserializer(resultSet.getLong(columnIndex++))
            }
        }

        val meta = if (receiveOptions.readMetadata && queue.metadata.fields.isNotEmpty()) {
            val metaValues = mutableListOf<MetaValue<*>>()

            queue.metadata.fields.forEach { metaField ->
                val metaValue = metaField.readValue(resultSet, columnIndex++)
                if (metaValue != null) {
                    metaValues += metaValue
                }
            }

            MetaValues.of(metaValues)
        } else {
            MetaValues.EMPTY
        }

        val otData = if (receiveOptions.readOpenTelemetryData) {
            // convert array [key1, value1, key2, value2...] into map {key1=value1, key2=value2...}
            @Suppress("UNCHECKED_CAST")
            val otArray = resultSet.getArray(columnIndex++)?.array as? Array<String>
            Helpers.arrayToMap(otArray)
        } else {
            null
        }

        val message = Message(
            id = Id(localId, shard),
            createdAt = createdAt,
            processingAt = processingAt,
            scheduledAt = scheduledAt,
            remainingAttempts = attempts,
            data = data,
            meta = meta
        )
        message.openTelemetryData = otData

        return message
    }

    fun generateDeleteQuery(queue: Queue<*>, ids: List<Id>): String {
        check(ids.isNotEmpty()) {
            "ID list must not be empty"
        }

        val idsList = ids.joinToString(separator = ",") { id ->
            "(${id.localId},${id.shard})"
        }

        return """
            with ids_as_values(${Const.ID_COLUMN_NAME},${Const.SHARD_COLUMN_NAME}) as (values $idsList)
            delete from ${queue.dbTableName}
            where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in (table ids_as_values)
        """
    }

    fun generateDeleteExpiredMessagesQuery(queue: Queue<*>, limit: Int): String {
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
