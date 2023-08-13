package kolbasa.producer

import kolbasa.queue.Queue
import kolbasa.queue.QueueDataType
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import kolbasa.utils.LongBox
import org.postgresql.util.PGobject
import java.sql.PreparedStatement

internal object ProducerSchemaHelpers {

    fun generateInsertPreparedQuery(
        queue: Queue<*, *>,
        producerOptions: ProducerOptions,
        data: List<SendMessage<*, *>>
    ): String {
        val columns = mutableListOf<String>()
        val values = Array<MutableList<String>>(data.size) { mutableListOf() }

        // delayMillis
        columns += Const.SCHEDULED_AT_COLUMN_NAME
        data.forEachIndexed { index, item ->
            val delay = QueueHelpers.calculateDelay(queue.options, item.sendOptions)
            values[index] += if (delay != null) {
                "clock_timestamp() + interval '${delay.toMillis()} millisecond'"
            } else {
                "null"
            }
        }

        // attempts
        columns += Const.REMAINING_ATTEMPTS_COLUMN_NAME
        data.forEachIndexed { index, item ->
            val remainingAttempts = QueueHelpers.calculateAttempts(queue.options, item.sendOptions)

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
        if (producerOptions.producer != null) {
            columns += Const.PRODUCER_COLUMN_NAME
            data.forEachIndexed { index, _ ->
                values[index] += "?"
            }
        }

        // data
        columns += Const.DATA_COLUMN_NAME
        data.forEachIndexed { index, _ ->
            values[index] += "?"
        }

        val columnsStr = columns.joinToString(separator = ",", prefix = "(", postfix = ")")
        val valuesStr = values.joinToString(separator = ",") {
            it.joinToString(separator = ",", prefix = "(", postfix = ")")
        }

        return """
            insert into ${queue.dbTableName} $columnsStr values $valuesStr returning id
        """.trimIndent()
    }

    fun <Data, Meta : Any> fillInsertPreparedQuery(
        queue: Queue<Data, Meta>,
        producerOptions: ProducerOptions,
        data: List<SendMessage<Data, Meta>>,
        preparedStatement: PreparedStatement,
        approxBytesCounter: LongBox
    ) {
        var columnIndex = 1

        data.forEach { item ->
            // All meta fields
            queue.metadataDescription?.fields?.forEach { field ->
                field.fillPreparedStatement(preparedStatement, columnIndex++, item.meta)
            }

            // producer name
            if (producerOptions.producer != null) {
                preparedStatement.setString(columnIndex++, producerOptions.producer)
            }

            // message data
            when (queue.dataType) {
                is QueueDataType.Json -> {
                    val jsonString = queue.dataType.serializer(item.data)
                    approxBytesCounter.inc(jsonString.length)

                    val jsonObject = PGobject()
                    jsonObject.type = queue.dataType.dbColumnType
                    jsonObject.value = jsonString
                    preparedStatement.setObject(columnIndex++, jsonObject)
                }

                is QueueDataType.Binary -> {
                    val binaryData = queue.dataType.serializer(item.data)
                    approxBytesCounter.inc(binaryData.size)
                    preparedStatement.setBytes(columnIndex++, binaryData)
                }

                is QueueDataType.Text -> {
                    val strData = queue.dataType.serializer(item.data)
                    approxBytesCounter.inc(strData.length)
                    preparedStatement.setString(columnIndex++, strData)
                }

                is QueueDataType.Int -> {
                    approxBytesCounter.inc(4)
                    preparedStatement.setInt(columnIndex++, queue.dataType.serializer(item.data))
                }

                is QueueDataType.Long -> {
                    approxBytesCounter.inc(8)
                    preparedStatement.setLong(columnIndex++, queue.dataType.serializer(item.data))
                }
            }
        }
    }
}
