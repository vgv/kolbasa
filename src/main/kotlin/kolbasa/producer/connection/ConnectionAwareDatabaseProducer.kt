package kolbasa.producer.connection

import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useSavepoint
import kolbasa.producer.*
import kolbasa.queue.Queue
import kolbasa.stats.prometheus.queuesize.QueueSizeHelper
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.BytesCounter
import kolbasa.utils.TimeHelper
import java.sql.Connection

/**
 * Default implementation of [ConnectionAwareProducer]
 */
class ConnectionAwareDatabaseProducer(
    private val producerOptions: ProducerOptions = ProducerOptions()
) : ConnectionAwareProducer {

    override fun <Data, Meta : Any> send(
        connection: Connection,
        queue: Queue<Data, Meta>,
        request: SendRequest<Data, Meta>
    ): SendResult<Data, Meta> {
        val approxStatsBytes = BytesCounter(queue.queueMetrics.usePreciseStringSize())
        val partialInsert = ProducerSchemaHelpers.calculatePartialInsert(producerOptions, request.sendOptions)

        val (execution, result) = TimeHelper.measure {
            queue.queueTracing.makeProducerCall(request) {
                when (partialInsert) {
                    PartialInsert.PROHIBITED -> sendProhibited(connection, queue, approxStatsBytes, request)
                    PartialInsert.UNTIL_FIRST_FAILURE -> sendUntilFirstFailure(connection, queue, approxStatsBytes, request)
                    PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> sendAsMuchAsPossible(connection, queue, approxStatsBytes, request)
                }
            }
        }

        // Prometheus
        queue.queueMetrics.producerSendMetrics(
            partialInsert,
            allMessages = request.data.size,
            failedMessages = result.failedMessages,
            executionNanos = execution.durationNanos,
            approxBytes = approxStatsBytes.get(),
            queueSizeCalcFunc = { QueueSizeHelper.calculateQueueLength(connection, queue) }
        )

        return result
    }

    private fun <Data, Meta : Any> sendProhibited(
        connection: Connection,
        queue: Queue<Data, Meta>,
        approxStatsBytes: BytesCounter,
        request: SendRequest<Data, Meta>
    ): SendResult<Data, Meta> {
        val result = ArrayList<MessageResult<Data, Meta>>(request.data.size)
        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, request.sendOptions)

        request.chunked(batchSize).forEach { chunk ->
            try {
                result += executeChunk(connection, queue, approxStatsBytes, chunk)
            } catch (e: Exception) {
                // just stop all sequence execution and return the result
                // all messages were failed
                return SendResult(failedMessages = request.data.size, listOf(MessageResult.Error(e, request.data)))
            }
        }

        // everything is ok, all chunks were inserted without any issues
        return SendResult(failedMessages = 0, result)
    }

    private fun <Data, Meta : Any> sendUntilFirstFailure(
        connection: Connection,
        queue: Queue<Data, Meta>,
        approxStatsBytes: BytesCounter,
        request: SendRequest<Data, Meta>
    ): SendResult<Data, Meta> {
        val results = ArrayList<MessageResult<Data, Meta>>(request.data.size)
        val failResults = mutableListOf<SendMessage<Data, Meta>>()
        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, request.sendOptions)
        lateinit var exception: Throwable

        request.chunked(batchSize).forEach { chunk ->
            if (failResults.isNotEmpty()) {
                // If we have at least one failed message – let's fail others
                failResults += chunk.data
            } else {
                executeChunkInSavepoint(connection, queue, approxStatsBytes, chunk)
                    .onSuccess { results += it }
                    .onFailure { ex ->
                        exception = ex
                        failResults += chunk.data
                    }
            }
        }

        if (failResults.isNotEmpty()) {
            results += MessageResult.Error(exception, failResults)
        }

        return SendResult(failedMessages = failResults.size, results)
    }

    private fun <Data, Meta : Any> sendAsMuchAsPossible(
        connection: Connection,
        queue: Queue<Data, Meta>,
        approxStatsBytes: BytesCounter,
        request: SendRequest<Data, Meta>
    ): SendResult<Data, Meta> {
        val result = ArrayList<MessageResult<Data, Meta>>(request.data.size)
        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, request.sendOptions)
        var failedMessages = 0

        request.chunked(batchSize).forEach { chunk ->
            executeChunkInSavepoint(connection, queue, approxStatsBytes, chunk)
                .onSuccess { result += it }
                .onFailure { ex ->
                    failedMessages += chunk.data.size
                    result += MessageResult.Error(ex, chunk.data)
                }
        }

        return SendResult(failedMessages, result)
    }

    private fun <Data, Meta : Any> executeChunkInSavepoint(
        connection: Connection,
        queue: Queue<Data, Meta>,
        approxStatsBytes: BytesCounter,
        request: SendRequest<Data, Meta>
    ): Result<List<MessageResult<Data, Meta>>> {
        return connection.useSavepoint { _ ->
            executeChunk(connection, queue, approxStatsBytes, request)
        }
    }

    private fun <Data, Meta : Any> executeChunk(
        connection: Connection,
        queue: Queue<Data, Meta>,
        approxStatsBytes: BytesCounter,
        request: SendRequest<Data, Meta>
    ): List<MessageResult<Data, Meta>> {
        val deduplicationMode = ProducerSchemaHelpers.calculateDeduplicationMode(producerOptions, request.sendOptions)
        val query = ProducerSchemaHelpers.generateInsertPreparedQuery(queue, producerOptions.producer, deduplicationMode, request)

        val (execution, result) = TimeHelper.measure {
            connection.usePreparedStatement(query) { preparedStatement ->
                ProducerSchemaHelpers.fillInsertPreparedQuery(
                    queue,
                    producerName = producerOptions.producer,
                    request,
                    preparedStatement,
                    approxStatsBytes
                )

                val result = ArrayList<MessageResult<Data, Meta>>(request.data.size)

                var currentIndex = 0
                preparedStatement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        val localId = resultSet.getLong(1)

                        when (deduplicationMode) {
                            DeduplicationMode.ERROR -> {
                                val id = Id(localId, request.effectiveShard)
                                result += MessageResult.Success(id = id, message = request.data[currentIndex++])
                            }

                            DeduplicationMode.IGNORE_DUPLICATES -> {
                                val realIndex = resultSet.getInt(2)
                                while (currentIndex < realIndex) {
                                    result += MessageResult.Duplicate(message = request.data[currentIndex++])
                                }

                                val id = Id(localId, request.effectiveShard)
                                result += MessageResult.Success(id = id, message = request.data[currentIndex++])
                            }
                        }
                    }
                }

                while (currentIndex < request.data.size) {
                    result += MessageResult.Duplicate(request.data[currentIndex++])
                }

                result
            }
        }

        // SQL dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.PRODUCER_INSERT, query, execution, result.size)

        return result
    }
}
