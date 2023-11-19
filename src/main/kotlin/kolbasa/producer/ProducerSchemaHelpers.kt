package kolbasa.producer

import kolbasa.queue.Queue
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import kolbasa.utils.BytesCounter
import org.postgresql.util.PGobject
import java.sql.PreparedStatement

internal object ProducerSchemaHelpers {

    fun generateInsertPreparedQuery(
        queue: Queue<*, *>,
        producerName: String?,
        deduplicationMode: DeduplicationMode,
        data: List<SendMessage<*, *>>
    ): String {
        val columns = mutableListOf<String>()
        val values = Array<MutableList<String>>(data.size) { mutableListOf() }

        // delayMillis
        columns += Const.SCHEDULED_AT_COLUMN_NAME
        data.forEachIndexed { index, item ->
            val delay = QueueHelpers.calculateDelay(queue.options, item.messageOptions)
            values[index] += if (delay != null) {
                "clock_timestamp() + interval '${delay.toMillis()} millisecond'"
            } else {
                "clock_timestamp()"
            }
        }

        // attempts
        columns += Const.REMAINING_ATTEMPTS_COLUMN_NAME
        data.forEachIndexed { index, item ->
            val remainingAttempts = QueueHelpers.calculateAttempts(queue.options, item.messageOptions)

            values[index] += "$remainingAttempts"
        }

        // meta fields
        if (queue.metadataDescription != null) {
            queue.metadataDescription.fields.forEach { field ->
                columns += field.dbColumnName
            }

            data.forEachIndexed { index, _ ->
                queue.metadataDescription.fields.forEach { _ ->
                    values[index] += "?"
                }
            }
        }

        // producer
        if (producerName != null) {
            columns += Const.PRODUCER_COLUMN_NAME
            data.forEachIndexed { index, _ ->
                values[index] += "?"
            }
        }

        // deduplication
        if (deduplicationMode == DeduplicationMode.IGNORE_DUPLICATES) {
            columns += Const.USELESS_COUNTER_COLUMN_NAME
            data.forEachIndexed { index, _ ->
                // just a sequence 0, 1, 2 etc.
                values[index] += index.toString()
            }
        }

        // data
        columns += Const.DATA_COLUMN_NAME
        data.forEachIndexed { index, _ ->
            values[index] += "?"
        }

        // Generate all query parts: columns, values, on conflict, returning etc.
        val columnsStr = columns.joinToString(separator = ",", prefix = "(", postfix = ")")
        val valuesStr = values.joinToString(separator = ",") {
            it.joinToString(separator = ",", prefix = "(", postfix = ")")
        }
        val onConflictStr = if (deduplicationMode == DeduplicationMode.IGNORE_DUPLICATES) {
            "on conflict do nothing"
        } else {
            ""
        }
        val returningColumns = if (deduplicationMode == DeduplicationMode.IGNORE_DUPLICATES) {
            "${Const.ID_COLUMN_NAME}, ${Const.USELESS_COUNTER_COLUMN_NAME}"
        } else {
            Const.ID_COLUMN_NAME
        }

        return "insert into ${queue.dbTableName}${columnsStr} values $valuesStr $onConflictStr returning $returningColumns"
    }

    fun <Data, Meta : Any> fillInsertPreparedQuery(
        queue: Queue<Data, Meta>,
        producerName: String?,
        data: List<SendMessage<Data, Meta>>,
        preparedStatement: PreparedStatement,
        approxBytesCounter: BytesCounter
    ) {
        var columnIndex = 1

        data.forEach { item ->
            // All meta fields
            queue.metadataDescription?.fields?.forEach { field ->
                field.fillPreparedStatement(preparedStatement, columnIndex++, item.meta)
            }

            // producer name
            if (producerName != null) {
                preparedStatement.setString(columnIndex++, producerName)
            }

            // message data
            when (queue.databaseDataType) {
                is DatabaseQueueDataType.Json -> {
                    val jsonString = queue.databaseDataType.serializer(item.data)
                    approxBytesCounter.addString(jsonString)

                    val jsonObject = PGobject()
                    jsonObject.type = queue.databaseDataType.dbColumnType
                    jsonObject.value = jsonString
                    preparedStatement.setObject(columnIndex++, jsonObject)
                }

                is DatabaseQueueDataType.Binary -> {
                    val binaryData = queue.databaseDataType.serializer(item.data)
                    approxBytesCounter.addByteArray(binaryData)
                    preparedStatement.setBytes(columnIndex++, binaryData)
                }

                is DatabaseQueueDataType.Text -> {
                    val strData = queue.databaseDataType.serializer(item.data)
                    approxBytesCounter.addString(strData)
                    preparedStatement.setString(columnIndex++, strData)
                }

                is DatabaseQueueDataType.Int -> {
                    approxBytesCounter.addInt()
                    preparedStatement.setInt(columnIndex++, queue.databaseDataType.serializer(item.data))
                }

                is DatabaseQueueDataType.Long -> {
                    approxBytesCounter.addLong()
                    preparedStatement.setLong(columnIndex++, queue.databaseDataType.serializer(item.data))
                }
            }
        }
    }

    fun calculateDeduplicationMode(producerOptions: ProducerOptions, sendOptions: SendOptions): DeduplicationMode {
        return if (sendOptions !== SendOptions.SEND_OPTIONS_NOT_SET) {
            sendOptions.deduplicationMode
        } else {
            producerOptions.deduplicationMode
        }
    }

    fun calculateBatchSize(producerOptions: ProducerOptions, sendOptions: SendOptions): Int {
        return if (sendOptions !== SendOptions.SEND_OPTIONS_NOT_SET) {
            sendOptions.batchSize
        } else {
            producerOptions.batchSize
        }
    }

    fun calculatePartialInsert(producerOptions: ProducerOptions, sendOptions: SendOptions): PartialInsert {
        return if (sendOptions !== SendOptions.SEND_OPTIONS_NOT_SET) {
            sendOptions.partialInsert
        } else {
            producerOptions.partialInsert
        }
    }
}
