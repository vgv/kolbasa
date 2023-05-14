package kolbasa.producer

import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useSavepoint
import kolbasa.queue.Queue
import kolbasa.stats.GlobalStats
import kolbasa.Kolbasa
import kolbasa.stats.QueueStats
import kolbasa.utils.LongBox
import java.sql.Connection

class ConnectionAwareDatabaseProducer<V, M : Any>(
    private val queue: Queue<V, M>,
    private val producerOptions: ProducerOptions = ProducerOptions()
) : ConnectionAwareProducer<V, M> {

    private val queueStats: QueueStats

    init {
        Kolbasa.registerQueue(queue)
        queueStats = GlobalStats.getStatsForQueue(queue)
    }

    override fun send(connection: Connection, data: V): Long {
        return send(connection, SendMessage(data))
    }

    override fun send(connection: Connection, data: SendMessage<V, M>): Long {
        val result = send(connection, listOf(data))

        return when (val message = result.messages.first()) {
            is MessageResult.Success -> message.id
            is MessageResult.Error -> throw message.error
        }
    }

    override fun send(connection: Connection, data: List<SendMessage<V, M>>): SendResult<V, M> {
        return when (producerOptions.partialInsert) {
            PartialInsert.PROHIBITED -> sendProhibited(connection, data)
            PartialInsert.UNTIL_FIRST_FAILURE -> sendUntilFirstFailure(connection, data)
            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> sendAsMuchAsPossible(connection, data)
        }
    }

    private fun sendProhibited(connection: Connection, data: List<SendMessage<V, M>>): SendResult<V, M> {
        val result = ArrayList<MessageResult<V, M>>(data.size)

        data.asSequence().chunked(producerOptions.batchSize).forEach { chunk ->
            try {
                result += executeChunk(connection, chunk)
            } catch (e: Exception) {
                // just stop all sequence execution and return the result
                return SendResult(data.size, listOf(MessageResult.Error(e, data)))
            }
        }

        // everything is ok, all chunks were inserted without any issues
        return SendResult(failedMessages = 0, result)
    }

    private fun sendUntilFirstFailure(connection: Connection, data: List<SendMessage<V, M>>): SendResult<V, M> {
        val results = ArrayList<MessageResult<V, M>>(data.size)
        val failResults = mutableListOf<SendMessage<V, M>>()
        lateinit var exception: Throwable

        data.asSequence().chunked(producerOptions.batchSize).forEach { chunk ->
            if (failResults.isNotEmpty()) {
                // If we have at least one failed message – let's fail others
                failResults += chunk
            } else {
                executeChunkInSavepoint(connection, chunk)
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

    private fun sendAsMuchAsPossible(connection: Connection, data: List<SendMessage<V, M>>): SendResult<V, M> {
        val result = ArrayList<MessageResult<V, M>>(data.size)
        var failedMessages = 0

        data.asSequence().chunked(producerOptions.batchSize).forEach { chunk ->
            executeChunkInSavepoint(connection, chunk)
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
        chunk: List<SendMessage<V, M>>
    ): Result<List<MessageResult<V, M>>> {
        return connection.useSavepoint { _ ->
            executeChunk(connection, chunk)
        }
    }

    private fun executeChunk(
        connection: Connection,
        chunk: List<SendMessage<V, M>>
    ): List<MessageResult<V, M>> {
        val approxStatsBytes = LongBox()
        val result = ArrayList<MessageResult<V, M>>(chunk.size)

        val query = ProducerSchemaHelpers.generateInsertPreparedQuery(queue, producerOptions, chunk)
        connection.usePreparedStatement(query) { preparedStatement ->
            ProducerSchemaHelpers.fillInsertPreparedQuery(
                queue,
                producerOptions,
                chunk,
                preparedStatement,
                approxStatsBytes
            )

            preparedStatement.executeQuery().use { resultSet ->
                var currentIndex = 0
                while (resultSet.next()) {
                    val message = chunk[currentIndex++]
                    val id = resultSet.getLong(1)
                    result += MessageResult.Success(id, message)
                }
            }
        }

        // stats
        queueStats.sendInc(calls = chunk.size.toLong(), bytes = approxStatsBytes.get())

        return result
    }

}
