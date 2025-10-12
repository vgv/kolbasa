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
        val query = MutatorSchemaHelpers.generateFilterQuery(queue, mutations, condition)

        var mutatedMessagesCount = 0
        var truncated = false
        var lastMessageId = IdRange.MIN_ID - 1
        var lastIterationRecords: Long
        val mutatedMessages = mutableListOf<MessageResult>()
        do {
            lastIterationRecords = 0

            connection.usePreparedStatement(query) { preparedStatement ->
                val columnIndex = ColumnIndex()
                preparedStatement.setLong(columnIndex.nextIndex(), lastMessageId)
                condition.fillPreparedQuery(queue, preparedStatement, columnIndex)
                preparedStatement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        val localId = resultSet.getLong(1)

                        mutatedMessagesCount++
                        lastIterationRecords++
                        if (lastMessageId < localId) {
                            lastMessageId = localId
                        }

                        if (mutatedMessagesCount <= mutatorOptions.maxMutatedMessagesKeepInMemory) {
                            val shard = resultSet.getInt(2)
                            val scheduledAt = resultSet.getTimestamp(3).time
                            val remainingAttempts = resultSet.getInt(4)

                            val id = Id(localId, shard)
                            mutatedMessages += MessageResult.Mutated(id, scheduledAt, remainingAttempts)
                            if (mutatedMessagesCount == mutatorOptions.maxMutatedMessagesKeepInMemory) {
                                truncated = true
                            }
                        }
                    }
                }
            }
        } while (lastMessageId < IdRange.MAX_ID && lastIterationRecords > 0)

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
                        val localId = resultSet.getLong(1)
                        val shard = resultSet.getInt(2)
                        val scheduledAt = resultSet.getTimestamp(3).time
                        val remainingAttempts = resultSet.getInt(4)

                        val id = Id(localId, shard)
                        mutateResult[id] = MessageResult.Mutated(id, scheduledAt, remainingAttempts)
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

}

