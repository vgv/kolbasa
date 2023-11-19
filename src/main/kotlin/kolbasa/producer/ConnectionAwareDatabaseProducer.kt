package kolbasa.producer

import kolbasa.Kolbasa
import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useSavepoint
import kolbasa.queue.Queue
import kolbasa.schema.Const
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.BytesCounter
import kolbasa.utils.TimeHelper
import java.sql.Connection

/**
 * Default implementation of [ConnectionAwareProducer]
 */
class ConnectionAwareDatabaseProducer<Data, Meta : Any>(
    private val queue: Queue<Data, Meta>,
    private val producerOptions: ProducerOptions = ProducerOptions()
) : ConnectionAwareProducer<Data, Meta> {

    override fun send(connection: Connection, data: Data): Long {
        return send(connection, SendMessage(data))
    }

    override fun send(connection: Connection, data: SendMessage<Data, Meta>): Long {
        val result = send(connection, listOf(data))

        return when (val message = result.messages.first()) {
            is MessageResult.Success -> message.id
            is MessageResult.Duplicate -> Const.RESERVED_DUPLICATE_ID
            is MessageResult.Error -> throw message.exception
        }
    }

    override fun send(connection: Connection, data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta> {
        return send(connection, data, SendOptions.SEND_OPTIONS_NOT_SET)
    }

    override fun send(
        connection: Connection,
        data: List<SendMessage<Data, Meta>>,
        sendOptions: SendOptions
    ): SendResult<Data, Meta> {
        val approxStatsBytes = BytesCounter(Kolbasa.prometheusConfig.preciseStringSize)
        val partialInsert = ProducerSchemaHelpers.calculatePartialInsert(producerOptions, sendOptions)

        val (execution, result) = TimeHelper.measure {
            when (partialInsert) {
                PartialInsert.PROHIBITED -> sendProhibited(connection, approxStatsBytes, data, sendOptions)
                PartialInsert.UNTIL_FIRST_FAILURE -> sendUntilFirstFailure(connection, approxStatsBytes, data, sendOptions)
                PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> sendAsMuchAsPossible(connection, approxStatsBytes, data, sendOptions)
            }
        }

        // Prometheus
        queue.queueMetrics.producerSendMetrics(
            partialInsert,
            allMessages = data.size,
            failedMessages = result.failedMessages,
            executionNanos = execution.durationNanos,
            approxBytes = approxStatsBytes.get()
        )

        return result
    }

    private fun sendProhibited(
        connection: Connection,
        approxStatsBytes: BytesCounter,
        data: List<SendMessage<Data, Meta>>,
        sendOptions: SendOptions
    ): SendResult<Data, Meta> {
        val result = ArrayList<MessageResult<Data, Meta>>(data.size)
        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, sendOptions)

        data.asSequence().chunked(batchSize).forEach { chunk ->
            try {
                result += executeChunk(connection, approxStatsBytes, chunk, sendOptions)
            } catch (e: Exception) {
                // just stop all sequence execution and return the result
                return SendResult(data.size, listOf(MessageResult.Error(e, data)))
            }
        }

        // everything is ok, all chunks were inserted without any issues
        return SendResult(failedMessages = 0, result)
    }

    private fun sendUntilFirstFailure(
        connection: Connection,
        approxStatsBytes: BytesCounter,
        data: List<SendMessage<Data, Meta>>,
        sendOptions: SendOptions
    ): SendResult<Data, Meta> {
        val results = ArrayList<MessageResult<Data, Meta>>(data.size)
        val failResults = mutableListOf<SendMessage<Data, Meta>>()
        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, sendOptions)
        lateinit var exception: Throwable

        data.asSequence().chunked(batchSize).forEach { chunk ->
            if (failResults.isNotEmpty()) {
                // If we have at least one failed message – let's fail others
                failResults += chunk
            } else {
                executeChunkInSavepoint(connection, approxStatsBytes, chunk, sendOptions)
                    .onSuccess { results += it }
                    .onFailure { ex ->
                        exception = ex
                        failResults += chunk
                    }
            }
        }

        if (failResults.isNotEmpty()) {
            results += MessageResult.Error(exception, failResults)
        }

        return SendResult(failedMessages = failResults.size, results)
    }

    private fun sendAsMuchAsPossible(
        connection: Connection,
        approxStatsBytes: BytesCounter,
        data: List<SendMessage<Data, Meta>>,
        sendOptions: SendOptions
    ): SendResult<Data, Meta> {
        val result = ArrayList<MessageResult<Data, Meta>>(data.size)
        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, sendOptions)
        var failedMessages = 0

        data.asSequence().chunked(batchSize).forEach { chunk ->
            executeChunkInSavepoint(connection, approxStatsBytes, chunk, sendOptions)
                .onSuccess { result += it }
                .onFailure { ex ->
                    failedMessages += chunk.size
                    result += MessageResult.Error(ex, chunk)
                }
        }

        return SendResult(failedMessages, result)
    }

    private fun executeChunkInSavepoint(
        connection: Connection,
        approxStatsBytes: BytesCounter,
        chunk: List<SendMessage<Data, Meta>>,
        sendOptions: SendOptions
    ): Result<List<MessageResult<Data, Meta>>> {
        return connection.useSavepoint { _ ->
            executeChunk(connection, approxStatsBytes, chunk, sendOptions)
        }
    }

    private fun executeChunk(
        connection: Connection,
        approxStatsBytes: BytesCounter,
        chunk: List<SendMessage<Data, Meta>>,
        sendOptions: SendOptions
    ): List<MessageResult<Data, Meta>> {
        val deduplicationMode = ProducerSchemaHelpers.calculateDeduplicationMode(producerOptions, sendOptions)
        val query = ProducerSchemaHelpers.generateInsertPreparedQuery(queue, producerOptions.producer, deduplicationMode, chunk)

        val (execution, result) = TimeHelper.measure {
            connection.usePreparedStatement(query) { preparedStatement ->
                ProducerSchemaHelpers.fillInsertPreparedQuery(
                    queue,
                    producerName = producerOptions.producer,
                    chunk,
                    preparedStatement,
                    approxStatsBytes
                )

                val result = ArrayList<MessageResult<Data, Meta>>(chunk.size)

                var currentIndex = 0
                preparedStatement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        val id = resultSet.getLong(1)

                        when (deduplicationMode) {
                            DeduplicationMode.ERROR -> {
                                result += MessageResult.Success(id, message = chunk[currentIndex++])
                            }

                            DeduplicationMode.IGNORE_DUPLICATES -> {
                                val realIndex = resultSet.getInt(2)
                                while (currentIndex < realIndex) {
                                    result += MessageResult.Duplicate(message = chunk[currentIndex++])
                                }

                                result += MessageResult.Success(id, message = chunk[currentIndex++])
                            }
                        }
                    }
                }

                while (currentIndex < chunk.size) {
                    result += MessageResult.Duplicate(chunk[currentIndex++])
                }

                result
            }
        }

        // SQL dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.PRODUCER_INSERT, query, execution, result.size)

        return result
    }

}
