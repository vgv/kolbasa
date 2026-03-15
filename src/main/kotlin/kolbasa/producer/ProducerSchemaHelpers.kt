package kolbasa.producer

import kolbasa.cluster.Shard
import kolbasa.cluster.ShardStrategy
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.Queue
import kolbasa.queue.QueueHelpers
import kolbasa.schema.Const
import kolbasa.utils.BytesCounter
import org.postgresql.util.PGobject
import java.sql.PreparedStatement
import java.util.concurrent.ExecutorService
import kotlin.math.abs

internal object ProducerSchemaHelpers {

    /**
     * Generates a fixed-shape INSERT query using UNNEST, for example:
     *
     * ```sql
     * insert into q_name(scheduled_at, remaining_attempts, shard, meta_field, data)
     * select (statement_timestamp() + (t.delay + t.ord) * interval '1 microsecond') as scheduled_at,
     *        t.remaining_attempts,
     *        ?::int as shard,
     *        t.meta_field,
     *        t.data
     * from unnest(?::bigint[], ?::int[], ?::int[], ?::bytea[])
     *   with ordinality as t(delay, remaining_attempts, meta_field, data, ord)
     * returning id
     * ```
     *
     * Per-message values (delay, attempts, meta fields, data) are passed as typed arrays via unnest.
     * Scalar values (shard, opentelemetry) are passed as regular parameters.
     * Producer name is embedded as a SQL literal.
     * WITH ORDINALITY provides row ordering (ord) used for scheduled_at uniqueness and uc derivation.
     */
    fun generateInsertPreparedQuery(
        queue: Queue<*>,
        producerOptions: ProducerOptions,
        deduplicationMode: DeduplicationMode,
        sendOptions: SendOptions,
        hasOpenTelemetry: Boolean
    ): String {
        val insertColumns = mutableListOf<String>()
        val selectExprs = mutableListOf<String>()
        val unnestParams = mutableListOf<String>()
        val unnestAliases = mutableListOf<String>()

        // scheduled_at - from delay array + ordinality
        insertColumns += Const.SCHEDULED_AT_COLUMN_NAME
        selectExprs += "(statement_timestamp() + (t.delay + t.ord) * interval '1 microsecond') as scheduled_at"
        unnestParams += "?::bigint[]"
        unnestAliases += "delay"

        // remaining_attempts - from array
        insertColumns += Const.REMAINING_ATTEMPTS_COLUMN_NAME
        selectExprs += "t.remaining_attempts"
        unnestParams += "?::int[]"
        unnestAliases += "remaining_attempts"

        // shard - scalar parameter
        insertColumns += Const.SHARD_COLUMN_NAME
        selectExprs += "?::int as shard"

        // meta fields - from arrays
        queue.metadata.fields.forEach { field ->
            insertColumns += field.dbColumnName
            selectExprs += "t.${field.dbColumnName}"
            unnestParams += "?::${field.dbColumnArrayBaseType}[]"
            unnestAliases += field.dbColumnName
        }

        // producer name - SQL literal
        val producerName = calculateProducerName(producerOptions, sendOptions)
        if (producerName != null) {
            insertColumns += Const.PRODUCER_COLUMN_NAME
            selectExprs += "'$producerName'"
        }

        // OpenTelemetry context propagation data - scalar parameter
        if (hasOpenTelemetry) {
            insertColumns += Const.OPENTELEMETRY_COLUMN_NAME
            selectExprs += "?::varchar[]"
        }

        // deduplication - uc derived from ordinality
        if (deduplicationMode == DeduplicationMode.IGNORE_DUPLICATE) {
            insertColumns += Const.USELESS_COUNTER_COLUMN_NAME
            selectExprs += "t.ord - 1"
        }

        // data - from array
        insertColumns += Const.DATA_COLUMN_NAME
        selectExprs += "t.data"
        unnestParams += "?::${queue.databaseDataType.dbColumnType}[]"
        unnestAliases += "data"

        // WITH ORDINALITY adds 'ord' column
        unnestAliases += "ord"

        // Generate all query parts
        val columnsStr = insertColumns.joinToString(separator = ",", prefix = "(", postfix = ")")
        val selectStr = selectExprs.joinToString(separator = ",")
        val unnestStr = unnestParams.joinToString(separator = ",")
        val aliasStr = unnestAliases.joinToString(separator = ",")

        val onConflictStr = if (deduplicationMode == DeduplicationMode.IGNORE_DUPLICATE) {
            " on conflict do nothing"
        } else {
            ""
        }
        val returningColumns = if (deduplicationMode == DeduplicationMode.IGNORE_DUPLICATE) {
            "${Const.ID_COLUMN_NAME}, ${Const.USELESS_COUNTER_COLUMN_NAME}"
        } else {
            Const.ID_COLUMN_NAME
        }

        return "insert into ${queue.dbTableName}${columnsStr} select $selectStr from unnest($unnestStr) with ordinality as t($aliasStr)$onConflictStr returning $returningColumns"
    }

    fun <Data> fillInsertPreparedQuery(
        queue: Queue<Data>,
        producerOptions: ProducerOptions,
        request: SendRequest<Data>,
        preparedStatement: PreparedStatement,
        approxBytesCounter: BytesCounter
    ) {
        val connection = preparedStatement.connection
        var columnIndex = 1

        // 1. Scalar: shard
        preparedStatement.setInt(columnIndex++, request.effectiveShard)

        // 2. Scalar: OpenTelemetry context (if present)
        request.openTelemetryContext?.let { otContext ->
            val otArray = connection.createArrayOf("varchar", otContext.toTypedArray())
            preparedStatement.setArray(columnIndex++, otArray)
        }

        // 3. Array: delays in microseconds (bigint[])
        val delays = Array(request.data.size) { index ->
            val item = request.data[index]
            val delay = QueueHelpers.calculateDelay(queue.options, producerOptions, request.sendOptions, item.messageOptions)
            durationToMicroseconds(delay)
        }
        preparedStatement.setArray(columnIndex++, connection.createArrayOf("bigint", delays))

        // 4. Array: remaining_attempts (int[])
        val attempts = Array(request.data.size) { index ->
            val item = request.data[index]
            QueueHelpers.calculateAttempts(queue.options, producerOptions, request.sendOptions, item.messageOptions)
        }
        preparedStatement.setArray(columnIndex++, connection.createArrayOf("int", attempts))

        // 5. Arrays: meta fields
        queue.metadata.fields.forEach { field ->
            val values = request.data.map { item -> item.meta.getOrNull(field) }
            field.fillPreparedStatementForValues(preparedStatement, columnIndex++, values)
        }

        // 6. Array: data
        val dataArray = when (queue.databaseDataType) {
            is DatabaseQueueDataType.Json -> {
                val dataValues = Array(request.data.size) { index ->
                    val jsonString = queue.databaseDataType.serializer(request.data[index].data)
                    approxBytesCounter.addString(jsonString)
                    val jsonObject = PGobject()
                    jsonObject.type = queue.databaseDataType.dbColumnType
                    jsonObject.value = jsonString
                    jsonObject
                }
                connection.createArrayOf(queue.databaseDataType.dbColumnType, dataValues)
            }

            is DatabaseQueueDataType.Binary -> {
                val dataValues = Array(request.data.size) { index ->
                    val binaryData = queue.databaseDataType.serializer(request.data[index].data)
                    approxBytesCounter.addByteArray(binaryData)
                    binaryData
                }
                connection.createArrayOf(queue.databaseDataType.dbColumnType, dataValues)
            }

            is DatabaseQueueDataType.Text -> {
                val dataValues = Array(request.data.size) { index ->
                    val strData = queue.databaseDataType.serializer(request.data[index].data)
                    approxBytesCounter.addString(strData)
                    strData
                }
                connection.createArrayOf(queue.databaseDataType.dbColumnType, dataValues)
            }

            is DatabaseQueueDataType.Int -> {
                val dataValues = Array(request.data.size) { index ->
                    approxBytesCounter.addInt()
                    queue.databaseDataType.serializer(request.data[index].data)
                }
                connection.createArrayOf(queue.databaseDataType.dbColumnType, dataValues)
            }

            is DatabaseQueueDataType.Long -> {
                val dataValues = Array(request.data.size) { index ->
                    approxBytesCounter.addLong()
                    queue.databaseDataType.serializer(request.data[index].data)
                }
                connection.createArrayOf(queue.databaseDataType.dbColumnType, dataValues)
            }
        }
        preparedStatement.setArray(columnIndex++, dataArray)
    }

    fun calculateProducerName(producerOptions: ProducerOptions, sendOptions: SendOptions): String? {
        return sendOptions.producer ?: producerOptions.producer
    }

    fun calculateDeduplicationMode(producerOptions: ProducerOptions, sendOptions: SendOptions): DeduplicationMode {
        return sendOptions.deduplicationMode ?: producerOptions.deduplicationMode
    }

    fun calculateBatchSize(producerOptions: ProducerOptions, sendOptions: SendOptions): Int {
        return sendOptions.batchSize ?: producerOptions.batchSize
    }

    fun calculatePartialInsert(producerOptions: ProducerOptions, sendOptions: SendOptions): PartialInsert {
        return sendOptions.partialInsert ?: producerOptions.partialInsert
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

    fun calculateAsyncExecutor(
        callExecutor: ExecutorService?,
        producerExecutor: ExecutorService?,
        defaultExecutor: ExecutorService
    ): ExecutorService {
        if (callExecutor != null) {
            return callExecutor
        }

        if (producerExecutor != null) {
            return producerExecutor
        }

        return defaultExecutor
    }

    private fun durationToMicroseconds(duration: java.time.Duration): Long {
        // Avoid duration.toNanos() which overflows for durations > ~292 years
        return duration.seconds * 1_000_000L + duration.nano.toLong() / 1_000
    }
}
