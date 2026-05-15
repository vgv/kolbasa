package kolbasa.consumer

import kolbasa.producer.Id

/**
 * Thrown by [ConsumerSchemaHelpers.read] when a queue row's payload cannot be deserialized into a [Message].
 *
 * The receive CTE has already updated `remaining_attempts` and `scheduled_at` for the row by the time
 * deserialization runs, so [kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer] catches this
 * exception and skips the row instead of letting it propagate. If it were rethrown, the surrounding
 * transaction would roll back the visibility-timeout bookkeeping and the poison message would be
 * re-selected on every tick, blocking the queue.
 */
class MessageDeserializationException(
    val id: Id,
    cause: Throwable
) : RuntimeException("Failed to deserialize message $id: ${cause.message}", cause)
