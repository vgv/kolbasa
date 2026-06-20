package kolbasa.consumer

import kolbasa.cluster.Shards
import kolbasa.producer.Id
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.Queue
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.MetaValue
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
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
            "${Const.SCHEDULED_AT_COLUMN_NAME} <= statement_timestamp()",
            "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}>0"
        )
        if (shards != Shards.ALL_SHARDS) {
            whereClauses += shards.asWhereClause
        }
        receiveOptions.filter?.let { filter ->
            whereClauses += filter.toSqlClause()
        }

        // ----------------------------------------------------------
        // 'order by' clauses
        val customColumnsForOrdering = mutableListOf<String>()
        val orderByClauses = mutableListOf<String>()
        // custom ordering clauses first, if any
        receiveOptions.order?.forEach { order ->
            orderByClauses += order.dbOrderClause
            if (order.field.dbColumnName !in dataColumns) {
                customColumnsForOrdering += order.field.dbColumnName
            }
        }
        // after custom clauses – standard
        orderByClauses += Const.SCHEDULED_AT_COLUMN_NAME

        // ----------------------------------------------------------

        val visibilityTimeout = QueueHelpers.calculateVisibilityTimeout(queue.options, consumerOptions, receiveOptions)

        return """
            with
            id_to_update_cte as (
                select ${Const.ID_COLUMN_NAME} as id_value from ${queue.dbTableName}
                where ${whereClauses.joinToString(separator = " and ")}
                order by ${orderByClauses.joinToString(separator = ",")}
                limit $limit
                for update skip locked
            ),
            updated_cte as (
                update ${queue.dbTableName}
                set
                    ${Const.PROCESSING_AT_COLUMN_NAME}=statement_timestamp(),
                    ${Const.SCHEDULED_AT_COLUMN_NAME}=statement_timestamp() + ${TimeHelper.generatePostgreSQLInterval(visibilityTimeout)},
                    ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}=${Const.REMAINING_ATTEMPTS_COLUMN_NAME}-1,
                    ${Const.CONSUMER_COLUMN_NAME}=?
                from id_to_update_cte where ${Const.ID_COLUMN_NAME} = id_to_update_cte.id_value
                returning ${(dataColumns + customColumnsForOrdering).joinToString(separator = ",")}
            )
            select ${dataColumns.joinToString(separator = ",")} from updated_cte
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
        val consumerName = calculateConsumerName(consumerOptions, receiveOptions)
        if (consumerName != null) {
            preparedStatement.setString(columnIndex.nextIndex(), consumerName)
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
            where ctid in (
                select ctid
                from ${queue.dbTableName}
                where
                    ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} <= 0 and
                    ${Const.SCHEDULED_AT_COLUMN_NAME} <= statement_timestamp()
                limit $limit
                for update skip locked
            )
        """.trimIndent()
    }

    fun generateMoveExpiredMessagesToDlqQuery(
        mainQueue: Queue<*>,
        dlq: Queue<*>,
        limit: Int
    ): String {
        // Columns to read from source via RETURNING
        val returningColumns = buildList {
            add(Const.ID_COLUMN_NAME)
            add(Const.SHARD_COLUMN_NAME)
            add(Const.CREATED_AT_COLUMN_NAME)
            add(Const.PROCESSING_AT_COLUMN_NAME)
            add(Const.SCHEDULED_AT_COLUMN_NAME)
            add(Const.PRODUCER_COLUMN_NAME)
            add(Const.DATA_COLUMN_NAME)
            mainQueue.metadata.fields.forEach { add(it.dbColumnName) }
        }
        val returningStr = returningColumns.joinToString(",")

        // Columns for DLQ insert — direct transfers + original-value meta fields
        val dlqInsertColumns = buildList {
            add(Const.SCHEDULED_AT_COLUMN_NAME)
            add(Const.REMAINING_ATTEMPTS_COLUMN_NAME)
            add(Const.SHARD_COLUMN_NAME)
            add(Const.PRODUCER_COLUMN_NAME)
            add(Const.DATA_COLUMN_NAME)
            mainQueue.metadata.fields.forEach { add(it.dbColumnName) }
            // Original-value meta fields
            add(Metadata.DLQ_ORIGINAL_ID.dbColumnName)
            add(Metadata.DLQ_ORIGINAL_CREATED_AT.dbColumnName)
            add(Metadata.DLQ_ORIGINAL_PROCESSING_AT.dbColumnName)
            add(Metadata.DLQ_ORIGINAL_SCHEDULED_AT.dbColumnName)
        }
        val dlqInsertColumnsStr = dlqInsertColumns.joinToString(",")

        // SELECT expressions — direct transfers + epoch-millis conversions for timestamps
        val selectExprs = buildList {
            add("statement_timestamp()")                            // scheduled_at = now
            add("${dlq.options.defaultAttempts}")                   // remaining_attempts
            add(Const.SHARD_COLUMN_NAME)
            add(Const.PRODUCER_COLUMN_NAME)
            add(Const.DATA_COLUMN_NAME)
            mainQueue.metadata.fields.forEach { add(it.dbColumnName) }
            // Original values
            add(Const.ID_COLUMN_NAME)               // original id
            add(Const.CREATED_AT_COLUMN_NAME)       // original created_at
            add(Const.PROCESSING_AT_COLUMN_NAME)    // original processing_at
            add(Const.SCHEDULED_AT_COLUMN_NAME)     // original scheduled_at
        }
        val selectStr = selectExprs.joinToString(",")

        return """
            with deleted_cte as (
                delete from ${mainQueue.dbTableName}
                where ctid in (
                    select ctid
                    from ${mainQueue.dbTableName}
                    where
                        ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} <= 0 and
                        ${Const.SCHEDULED_AT_COLUMN_NAME} <= statement_timestamp()
                    limit $limit
                    for update skip locked
                )
                returning $returningStr
            )
            insert into ${dlq.dbTableName} ($dlqInsertColumnsStr)
            select $selectStr
            from deleted_cte
        """.trimIndent()
    }

    fun generateMoveDeletedMessagesToArchiveQuery(
        mainQueue: Queue<*>,
        archiveQueue: Queue<*>,
        ids: List<Id>
    ): String {
        check(ids.isNotEmpty()) { "ID list must not be empty" }

        val idsList = ids.joinToString(separator = ",") { id -> "(${id.localId},${id.shard})" }

        // Columns to read from source via RETURNING
        val returningColumns = buildList {
            add(Const.ID_COLUMN_NAME)
            add(Const.SHARD_COLUMN_NAME)
            add(Const.CREATED_AT_COLUMN_NAME)
            add(Const.REMAINING_ATTEMPTS_COLUMN_NAME)
            add(Const.PROCESSING_AT_COLUMN_NAME)
            add(Const.PRODUCER_COLUMN_NAME)
            add(Const.CONSUMER_COLUMN_NAME)
            add(Const.DATA_COLUMN_NAME)
            mainQueue.metadata.fields.forEach { add(it.dbColumnName) }
        }
        val returningStr = returningColumns.joinToString(",")

        // Columns for Archive insert
        val arcInsertColumns = buildList {
            add(Const.SCHEDULED_AT_COLUMN_NAME)
            add(Const.REMAINING_ATTEMPTS_COLUMN_NAME)
            add(Const.SHARD_COLUMN_NAME)
            add(Const.PRODUCER_COLUMN_NAME)
            add(Const.CONSUMER_COLUMN_NAME)
            add(Const.DATA_COLUMN_NAME)
            mainQueue.metadata.fields.forEach { add(it.dbColumnName) }
            // Original-value meta fields
            add(Metadata.ARCHIVE_ORIGINAL_ID.dbColumnName)
            add(Metadata.ARCHIVE_ORIGINAL_CREATED_AT.dbColumnName)
            add(Metadata.ARCHIVE_ORIGINAL_REMAINING_ATTEMPTS.dbColumnName)
            add(Metadata.ARCHIVE_ORIGINAL_PROCESSING_AT.dbColumnName)
        }
        val arcInsertColumnsStr = arcInsertColumns.joinToString(",")

        // SELECT expressions
        val selectExprs = buildList {
            add("statement_timestamp()")                       // scheduled_at = now
            add("${archiveQueue.options.defaultAttempts}")     // remaining_attempts (high value)
            add(Const.SHARD_COLUMN_NAME)
            add(Const.PRODUCER_COLUMN_NAME)
            add(Const.CONSUMER_COLUMN_NAME)
            add(Const.DATA_COLUMN_NAME)
            mainQueue.metadata.fields.forEach { add(it.dbColumnName) }
            // Original values
            add(Const.ID_COLUMN_NAME)                    // original id
            add(Const.CREATED_AT_COLUMN_NAME)            // original created_at
            add(Const.REMAINING_ATTEMPTS_COLUMN_NAME)    // original remaining_attempts
            add(Const.PROCESSING_AT_COLUMN_NAME)         // original processing_at
        }
        val selectStr = selectExprs.joinToString(",")

        return """
            with
            ids_as_values(${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) as (values $idsList),
            deleted_cte as (
                delete from ${mainQueue.dbTableName}
                where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in (table ids_as_values)
                returning $returningStr
            )
            insert into ${archiveQueue.dbTableName} ($arcInsertColumnsStr)
            select $selectStr
            from deleted_cte
        """.trimIndent()
    }

    fun calculateConsumerName(consumerOptions: ConsumerOptions, receiveOptions: ReceiveOptions): String? {
        return receiveOptions.consumer ?: consumerOptions.consumer
    }

}
