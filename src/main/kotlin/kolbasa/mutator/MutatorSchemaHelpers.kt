package kolbasa.mutator

import kolbasa.consumer.filter.Condition
import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.schema.Const
import kolbasa.utils.TimeHelper

internal object MutatorSchemaHelpers {

    fun generateListMutateQuery(
        queue: Queue<*>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): String {
        check(messages.isNotEmpty()) {
            "ID list must not be empty"
        }

        val mutatedFields = generateMutateExpressions(mutations)

        val idsList = messages.joinToString(separator = ",") { message ->
            "(${message.localId},${message.shard})"
        }

        return """
            with
                ids_as_values(${Const.ID_COLUMN_NAME},${Const.SHARD_COLUMN_NAME}) as (values $idsList)
            update ${queue.dbTableName}
            set
                ${mutatedFields.joinToString(separator = ",")}
            where
                (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in (table ids_as_values)
            returning $ALL_FIELDS"""
    }

    fun generateFilterQuery(
        queue: Queue<*>,
        mutations: List<Mutation>,
        condition: Condition,
        lastKnownId: Long,
        returnAllFields: Boolean
    ): String {
        val clauses = generateMutateExpressions(mutations)
        // meta_field_1 = ? and meta_field_2 > ? and ...
        val query = condition.toSqlClause()
        val fields = if (returnAllFields) {
            ALL_FIELDS
        } else {
            REDUCED_FIELDS
        }

        return """
            update ${queue.dbTableName}
            set
                ${clauses.joinToString(separator = ",")}
            where
                ${Const.ID_COLUMN_NAME} in (
                    select ${Const.ID_COLUMN_NAME}
                    from ${queue.dbTableName}
                    where (${Const.ID_COLUMN_NAME} > $lastKnownId) and ($query)
                    order by ${Const.ID_COLUMN_NAME}
                    limit $MUTATE_BATCH_SIZE
                 )
            returning $fields"""
    }

    private fun generateMutateExpression(mutation: Mutation): String {
        return when (mutation) {
            is AddRemainingAttempts -> {
                "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}=${Const.REMAINING_ATTEMPTS_COLUMN_NAME} + ${mutation.delta}"
            }

            is SetRemainingAttempts -> {
                "${Const.REMAINING_ATTEMPTS_COLUMN_NAME}=${mutation.newValue}"
            }

            is AddScheduledAt -> {
                val scheduledAt = Const.SCHEDULED_AT_COLUMN_NAME
                "${scheduledAt}=${scheduledAt} + ${TimeHelper.generatePostgreSQLInterval(mutation.delta)}"
            }

            is SetScheduledAt -> {
                val scheduledAt = Const.SCHEDULED_AT_COLUMN_NAME
                "${scheduledAt}=clock_timestamp() + ${TimeHelper.generatePostgreSQLInterval(mutation.newValue)}"
            }
        }
    }

    private fun generateMutateExpressions(mutations: List<Mutation>): List<String> {
        return mutations.map(::generateMutateExpression)
    }

    private const val REDUCED_FIELDS =
        Const.ID_COLUMN_NAME

    private const val ALL_FIELDS =
        "${Const.ID_COLUMN_NAME},${Const.SHARD_COLUMN_NAME},${Const.SCHEDULED_AT_COLUMN_NAME},${Const.REMAINING_ATTEMPTS_COLUMN_NAME}"

    private const val MUTATE_BATCH_SIZE = 10_000
}
