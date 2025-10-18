package kolbasa.mutator.connection

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.*
import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.Id
import kolbasa.queue.Checks
import kolbasa.queue.Queue
import kolbasa.schema.IdRange
import kolbasa.schema.NodeId
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.ColumnIndex
import kolbasa.utils.TimeHelper
import java.sql.Connection
import java.sql.ResultSet
import kotlin.math.min

class ConnectionAwareDatabaseMutator internal constructor(
    internal val nodeId: NodeId,
    internal val mutatorOptions: MutatorOptions = MutatorOptions()
) : ConnectionAwareMutator {

    @JvmOverloads
    constructor(mutatorOptions: MutatorOptions = MutatorOptions()) : this(
        nodeId = NodeId.EMPTY_NODE_ID,
        mutatorOptions = mutatorOptions
    )

    override fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult {
        if (messages.isEmpty() || mutations.isEmpty()) {
            return MutateResult(mutatedMessages = 0, truncated = false, emptyList())
        }

        Checks.checkMutations(mutations)

        val mutateResult = internalMutateById(connection, queue, mutations, messages)

        // combine result using the same order
        var mutatedMessagesCount = 0
        val mutatedMessages = messages.map { id ->
            val result = mutateResult[id]
            if (result != null) {
                mutatedMessagesCount++
                result
            } else {
                MessageResult.NotFound(id)
            }
        }

        return MutateResult(mutatedMessages = mutatedMessagesCount, truncated = false, messages = mutatedMessages)
    }

    override fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult {
        if (mutations.isEmpty()) {
            return MutateResult(mutatedMessages = 0, truncated = false, emptyList())
        }

        Checks.checkMutations(mutations)

        val condition = filter(Filter)

        var mutatedMessagesCount = 0
        var iterations = 0
        var truncated = false
        var lastMessageId = IdRange.MIN_ID - 1
        val mutatedMessages = mutableListOf<MessageResult>()
        val (execution, _) = TimeHelper.measure {
            do {
                val (maxSeenId, processedMessagesCount, processedMessages) = internalMutateByFilterOneIteration(
                    connection,
                    queue,
                    mutations,
                    condition,
                    lastMessageId,
                    !truncated
                )

                lastMessageId = maxSeenId
                mutatedMessagesCount += processedMessagesCount
                iterations++

                // copy processed messages
                val currentSize = mutatedMessages.size
                val maxSize = mutatorOptions.maxMutatedMessagesKeepInMemory
                if (currentSize < maxSize) {
                    val needToCopy = maxSize - currentSize
                    mutatedMessages += processedMessages.subList(0, min(processedMessages.size, needToCopy))
                }

                truncated = mutatedMessages.size >= mutatorOptions.maxMutatedMessagesKeepInMemory
            } while (processedMessagesCount > 0 && lastMessageId < IdRange.MAX_ID)
        }

        // Prometheus
        queue.queueMetrics.mutatorMetrics(
            nodeId,
            iterations = iterations,
            mutatedMessages = mutatedMessagesCount,
            executionNanos = execution.durationNanos,
            byId = false
        )

        return MutateResult(mutatedMessages = mutatedMessagesCount, truncated = truncated, mutatedMessages)
    }

    private fun <Data, Meta : Any> internalMutateById(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): Map<Id, MessageResult> {
        val query = MutatorSchemaHelpers.generateListMutateQuery(queue, mutations, messages)

        val (execution, mutateResult) = TimeHelper.measure {
            connection.useStatement { statement ->
                statement.executeQuery(query).use { resultSet ->
                    val mutateResult = mutableMapOf<Id, MessageResult>()

                    while (resultSet.next()) {
                        val mutatedMessage = readAllFields(resultSet)
                        mutateResult[mutatedMessage.id] = mutatedMessage
                    }

                    mutateResult
                }
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.MUTATE_BY_ID, query, execution, mutateResult.size)

        // Prometheus
        queue.queueMetrics.mutatorMetrics(
            nodeId,
            iterations = 1,
            mutatedMessages = mutateResult.size,
            executionNanos = execution.durationNanos,
            byId = true
        )

        return mutateResult
    }

    private fun <Data, Meta : Any> internalMutateByFilterOneIteration(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        condition: Condition<Meta>,
        lastKnownId: Long,
        returnFullResponse: Boolean
    ): Res {
        val query = MutatorSchemaHelpers.generateFilterQuery(queue, mutations, condition, lastKnownId, returnFullResponse)

        var maxSeenId = lastKnownId
        var processedMessagesCount = 0
        val processedMessages = mutableListOf<MessageResult>()

        val (execution, _) = TimeHelper.measure {
            connection.usePreparedStatement(query) { preparedStatement ->
                val columnIndex = ColumnIndex()
                condition.fillPreparedQuery(queue, preparedStatement, columnIndex)
                preparedStatement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        val localId = if (returnFullResponse) {
                            val mutatedMessage = readAllFields(resultSet)
                            processedMessages += mutatedMessage
                            mutatedMessage.id.localId
                        } else {
                            readReducedFields(resultSet)
                        }

                        if (maxSeenId < localId) {
                            maxSeenId = localId
                        }

                        processedMessagesCount++
                    }
                }
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.MUTATE_BY_FILTER, query, execution, processedMessagesCount)

        return Res(maxSeenId, processedMessagesCount, processedMessages)
    }

    private fun readReducedFields(resultSet: ResultSet): Long {
        return resultSet.getLong(1)
    }

    private fun readAllFields(resultSet: ResultSet): MessageResult.Mutated {
        val localId = resultSet.getLong(1)
        val shard = resultSet.getInt(2)
        val scheduledAt = resultSet.getTimestamp(3).time
        val remainingAttempts = resultSet.getInt(4)

        val id = Id(localId, shard)
        return MessageResult.Mutated(id, scheduledAt, remainingAttempts)
    }

    private data class Res(
        val maxSeenId: Long,
        val processedMessagesCount: Int,
        val processedMessages: List<MessageResult>
    )


}

