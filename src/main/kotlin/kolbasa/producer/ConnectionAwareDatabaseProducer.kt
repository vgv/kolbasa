package kolbasa.producer

import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useSavepoint
import kolbasa.queue.Queue
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.BytesCounter
import kolbasa.utils.TimeHelper
import java.sql.Connection

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
            is MessageResult.Error -> throw message.error
        }
    }

    override fun send(connection: Connection, data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta> {
        val approxStatsBytes = BytesCounter()
        val (execution, result) = TimeHelper.measure {
            when (producerOptions.partialInsert) {
                PartialInsert.PROHIBITED -> sendProhibited(connection, approxStatsBytes, data)
                PartialInsert.UNTIL_FIRST_FAILURE -> sendUntilFirstFailure(connection, approxStatsBytes, data)
                PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> sendAsMuchAsPossible(connection, approxStatsBytes, data)
            }
        }

        // Prometheus
        queue.queueMetrics.producerSendMetrics(
            producerOptions.partialInsert,
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
        data: List<SendMessage<Data, Meta>>
    ): SendResult<Data, Meta> {
        val result = ArrayList<MessageResult<Data, Meta>>(data.size)

        data.asSequence().chunked(producerOptions.batchSize).forEach { chunk ->
            try {
                result += executeChunk(connection, approxStatsBytes, chunk)
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
        data: List<SendMessage<Data, Meta>>
    ): SendResult<Data, Meta> {
        val results = ArrayList<MessageResult<Data, Meta>>(data.size)
        val failResults = mutableListOf<SendMessage<Data, Meta>>()
        lateinit var exception: Throwable

        data.asSequence().chunked(producerOptions.batchSize).forEach { chunk ->
            if (failResults.isNotEmpty()) {
                // If we have at least one failed message – let's fail others
                failResults += chunk
            } else {
                executeChunkInSavepoint(connection, approxStatsBytes, chunk)
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
        data: List<SendMessage<Data, Meta>>
    ): SendResult<Data, Meta> {
        val result = ArrayList<MessageResult<Data, Meta>>(data.size)
        var failedMessages = 0

        data.asSequence().chunked(producerOptions.batchSize).forEach { chunk ->
            executeChunkInSavepoint(connection, approxStatsBytes, chunk)
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
        chunk: List<SendMessage<Data, Meta>>
    ): Result<List<MessageResult<Data, Meta>>> {
        return connection.useSavepoint { _ ->
            executeChunk(connection, approxStatsBytes, chunk)
        }
    }

    private fun executeChunk(
        connection: Connection,
        approxStatsBytes: BytesCounter,
        chunk: List<SendMessage<Data, Meta>>
    ): List<MessageResult<Data, Meta>> {
        val query = ProducerSchemaHelpers.generateInsertPreparedQuery(queue, producerOptions, chunk)

        val (execution, result) = TimeHelper.measure {
            connection.usePreparedStatement(query) { preparedStatement ->
                ProducerSchemaHelpers.fillInsertPreparedQuery(
                    queue,
                    producerOptions,
                    chunk,
                    preparedStatement,
                    approxStatsBytes
                )

                val result = ArrayList<MessageResult<Data, Meta>>(chunk.size)

                preparedStatement.executeQuery().use { resultSet ->
                    var currentIndex = 0
                    while (resultSet.next()) {
                        val message = chunk[currentIndex++]
                        val id = resultSet.getLong(1)
                        result += MessageResult.Success(id, message)
                    }
                }

                result
            }
        }

        // SQL dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.PRODUCER_INSERT, query, execution, result.size)

        return result
    }

}
