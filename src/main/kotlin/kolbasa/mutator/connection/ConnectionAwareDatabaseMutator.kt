package kolbasa.mutator.connection

import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
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
        messages: List<Id>,
        mutations: List<Mutation>
    ): MutateResult {
        if (messages.isEmpty() || mutations.isEmpty()) {
            return MutateResult()
        }

        Checks.checkMutations(mutations)

        val clauses = mutations.map { mutation ->
            when (mutation) {
                is AddRemainingAttempts -> {
                    "${Const.REMAINING_ATTEMPTS_COLUMN_NAME} = ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} + ${mutation.delta}"
                }

                is SetRemainingAttempts -> {
                    "${Const.REMAINING_ATTEMPTS_COLUMN_NAME} = ${mutation.newValue}"
                }

                is AddScheduledAt -> {
                    val scheduledAt = Const.SCHEDULED_AT_COLUMN_NAME
                    "$scheduledAt = $scheduledAt + interval '${mutation.delta.toMillis()} millisecond'"
                }

                is SetScheduledAt -> {
                    "${Const.SCHEDULED_AT_COLUMN_NAME} = clock_timestamp() + interval '${mutation.newValue.toMillis()} millisecond'"
                }
            }
        }

        val idsList = messages.joinToString(separator = ",", prefix = "(", postfix = ")") { message ->
            "(${message.localId},${message.shard})"
        }

        val sql = """
            update ${queue.dbTableName}
            set
                ${clauses.joinToString(separator = ",")}
            where
                (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in $idsList
        """.trimIndent()

        val rows = connection.useStatement { statement ->
            statement.executeUpdate(sql)
        }

        return MutateResult()
    }
}
