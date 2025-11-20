package kolbasa.producer

import kolbasa.cluster.Shard
import kolbasa.cluster.ShardStrategy
import kolbasa.queue.Queue
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.QueueHelpers
import kolbasa.queue.meta.MetaField
import kolbasa.schema.Const
import kolbasa.utils.BytesCounter
import kolbasa.utils.TimeHelper
import org.postgresql.util.PGobject
import java.sql.PreparedStatement
import java.util.concurrent.ExecutorService
import kotlin.math.abs

internal object ProducerSchemaHelpers {

    fun generateInsertPreparedQuery(
        queue: Queue<*>,
        producerName: String?,
        deduplicationMode: DeduplicationMode,
        request: SendRequest<*>
    ): String {
        val columns = mutableListOf<String>()
        val values = Array<MutableList<String>>(request.data.size) { mutableListOf() }

        // delayMillis
        columns += Const.SCHEDULED_AT_COLUMN_NAME
        request.data.forEachIndexed { index, item ->
            val delay = QueueHelpers.calculateDelay(queue.options, item.messageOptions)
            values[index] += if (delay != null) {
                "clock_timestamp() + ${TimeHelper.generatePostgreSQLInterval(delay)}"
            } else {
                "clock_timestamp()"
            }
        }

        // attempts
        columns += Const.REMAINING_ATTEMPTS_COLUMN_NAME
        request.data.forEachIndexed { index, item ->
            val remainingAttempts = QueueHelpers.calculateAttempts(queue.options, item.messageOptions)

            values[index] += "$remainingAttempts"
        }

        // shard
        columns += Const.SHARD_COLUMN_NAME
        request.data.forEachIndexed { index, _ ->
            values[index] += "${request.effectiveShard}"
        }

        // meta fields
        queue.metadata.fields.forEach { field ->
            columns += field.dbColumnName
        }

        request.data.forEachIndexed { index, _ ->
            queue.metadata.fields.forEach { _ ->
                values[index] += "?"
            }
        }

        // producer name
        if (producerName != null) {
            columns += Const.PRODUCER_COLUMN_NAME
            request.data.forEachIndexed { index, _ ->
                values[index] += "?"
            }
        }

        // OpenTelemetry context propagation data
        if (request.openTelemetryContext != null) {
            columns += Const.OPENTELEMETRY_COLUMN_NAME
            request.data.forEachIndexed { index, _ ->
                values[index] += "?"
            }
        }

        // deduplication
        if (deduplicationMode == DeduplicationMode.IGNORE_DUPLICATES) {
            columns += Const.USELESS_COUNTER_COLUMN_NAME
            request.data.forEachIndexed { index, _ ->
                // just a sequence 0, 1, 2 etc.
                values[index] += index.toString()
            }
        }

        // data
        columns += Const.DATA_COLUMN_NAME
        request.data.forEachIndexed { index, _ ->
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

    fun <Data> fillInsertPreparedQuery(
        queue: Queue<Data>,
        producerName: String?,
        request: SendRequest<Data>,
        preparedStatement: PreparedStatement,
        approxBytesCounter: BytesCounter
    ) {
        // If we have OT data – let's create PG array once and use it for all rows
        val openTelemetryData = request.openTelemetryContext?.let { context ->
            preparedStatement.connection.createArrayOf("varchar", context.toTypedArray())
        }

        var columnIndex = 1

        request.data.forEach { item ->
            // All meta fields
            queue.metadata.fields.forEach { field: MetaField<*> ->
                field.fillPreparedStatementForValue(preparedStatement, columnIndex++, item.meta)
            }

            // producer name
            if (producerName != null) {
                preparedStatement.setString(columnIndex++, producerName)
            }

            if (openTelemetryData != null) {
                preparedStatement.setArray(columnIndex++, openTelemetryData)
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

    fun calculateEffectiveShard(sendOptions: SendOptions, producerOptions: ProducerOptions, shardStrategy: ShardStrategy): Int {
        if (sendOptions.shard != null) {
            return abs(sendOptions.shard % Shard.SHARD_COUNT)
        }

        if (producerOptions.shard != null) {
            return abs(producerOptions.shard % Shard.SHARD_COUNT)
        }

        return abs(shardStrategy.getShard() % Shard.SHARD_COUNT)
    }

    fun calculateAsyncExecutor(customExecutor: ExecutorService?, defaultExecutor: ExecutorService): ExecutorService {
        if (customExecutor != null) {
            return customExecutor
        }

        return defaultExecutor
    }
}
