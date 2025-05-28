package kolbasa.mutator.connection

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
import kolbasa.mutator.MessageResult
import kolbasa.mutator.MutateResult
import kolbasa.mutator.Mutation
import kolbasa.mutator.SetRemainingAttempts
import kolbasa.mutator.SetScheduledAt
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.Id
import kolbasa.queue.Checks
import kolbasa.queue.Queue
import kolbasa.schema.Const
import java.sql.Connection

class ConnectionAwareDatabaseMutator : ConnectionAwareMutator {

    override fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult {
        if (messages.isEmpty() || mutations.isEmpty()) {
            return MutateResult(mutatedMessages = 0, emptyList())
        }

        Checks.checkMutations(mutations)

        val query = generateMutateQuery(queue, messages, mutations)

        val mutateResult = mutableMapOf<Id, MessageResult>()
        connection.useStatement { statement ->
            statement.executeQuery(query).use { resultSet ->
                while (resultSet.next()) {
                    val localId = resultSet.getLong(1)
                    val shard = resultSet.getInt(2)
                    val scheduledAt = resultSet.getTimestamp(3).time
                    val remainingAttempts = resultSet.getInt(4)

                    val id = Id(localId, shard)
                    mutateResult[id] = MessageResult.Mutated(id, scheduledAt, remainingAttempts)
                }
            }
        }

        // combine result using the same order
        var mutatedMessages = 0
        val a = messages.map { id ->
            val result = mutateResult[id]
            if (result != null) {
                mutatedMessages++
                result
            } else {
                MessageResult.NotFound(id)
            }
        }

        return MutateResult(mutatedMessages = mutatedMessages, a)
    }

    override fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult {
        TODO("Not yet implemented")
    }

    private fun generateMutateQuery(
        queue: Queue<*, *>,
        messages: List<Id>,
        mutations: List<Mutation>
    ): String {
        val clauses = mutations.map { mutation ->
            when (mutation) {
                is AddRemainingAttempts -> {
                    "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}=${Const.REMAINING_ATTEMPTS_COLUMN_NAME} + ${mutation.delta}"
                }

                is SetRemainingAttempts -> {
                    "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}=${mutation.newValue}"
                }

                is AddScheduledAt -> {
                    val scheduledAt = Const.SCHEDULED_AT_COLUMN_NAME
                    val millis = mutation.delta.toMillis()
                    "${scheduledAt}=${scheduledAt} + interval '$millis millisecond'"
                }

                is SetScheduledAt -> {
                    val scheduledAt = Const.SCHEDULED_AT_COLUMN_NAME
                    val millis = mutation.newValue.toMillis()
                    "${scheduledAt}=clock_timestamp() + interval '$millis millisecond'"
                }
            }
        }

        val idsList = messages.joinToString(separator = ",", prefix = "(", postfix = ")") { message ->
            "(${message.localId},${message.shard})"
        }

        return """
            update ${queue.dbTableName}
            set
                ${clauses.joinToString(separator = ",")}
            where
                (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in $idsList
            returning
                ${Const.ID_COLUMN_NAME},
                ${Const.SHARD_COLUMN_NAME},
                ${Const.SCHEDULED_AT_COLUMN_NAME},
                ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
        """.trimIndent()
    }
}
