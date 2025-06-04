package kolbasa.mutator

import kolbasa.consumer.filter.Condition
import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.schema.Const

internal object MutatorSchemaHelpers {

    fun generateListMutateQuery(
        queue: Queue<*, *>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): String {
        check(messages.isNotEmpty()) {
            "ID list must not be empty"
        }

        val mutatedFields = generateMutateExpressions(mutations)

        val idsList = messages.joinToString(separator = ",", prefix = "(", postfix = ")") { message ->
            "(${message.localId},${message.shard})"
        }

        return """
            update ${queue.dbTableName}
            set
                ${mutatedFields.joinToString(separator = ",")}
            where
                (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in $idsList
            returning
                ${Const.ID_COLUMN_NAME},
                ${Const.SHARD_COLUMN_NAME},
                ${Const.SCHEDULED_AT_COLUMN_NAME},
                ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
        """
    }

    fun <Meta : Any> generateFilterQuery(
        queue: Queue<*, Meta>,
        mutations: List<Mutation>,
        condition: Condition<Meta>
    ): String {
        val clauses = generateMutateExpressions(mutations)
        // meta_field_1 = ? and meta_field_2 > ? and ...
        val query = condition.toSqlClause(queue)

        return """
            update ${queue.dbTableName}
            set
                ${clauses.joinToString(separator = ",")}
            where
                ${Const.ID_COLUMN_NAME} in (
                    select ${Const.ID_COLUMN_NAME}
                    from ${queue.dbTableName}
                    where ${Const.ID_COLUMN_NAME} > ? and $query
                    order by ${Const.ID_COLUMN_NAME}
                    limit $MUTATE_BATCH_SIZE
                 )
            returning
                ${Const.ID_COLUMN_NAME},
                ${Const.SHARD_COLUMN_NAME},
                ${Const.SCHEDULED_AT_COLUMN_NAME},
                ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
            """
    }

    fun generateMutateExpression(mutation: Mutation): String {
        return when (mutation) {
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

    fun generateMutateExpressions(mutations: List<Mutation>): List<String> {
        return mutations.map(::generateMutateExpression)
    }


    private const val MUTATE_BATCH_SIZE = 10_000
}
