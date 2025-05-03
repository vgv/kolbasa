package kolbasa.mutator

import java.time.Duration

/**
 * Marker sealed interface for all possible mutations
 */
sealed interface Mutation

/**
 * Adds `delta` to the current remaining attempts of the messages
 *
 * SQL: `remaining_attempts = remaining_attempts + delta`
 */
data class AddRemainingAttempts(val delta: Int) : Mutation, MutationField.RemainingAttemptField

/**
 * Sets remaining attempts of the messages to `newValue`
 *
 * SQL: `remaining_attempts = newValue`
 */
data class SetRemainingAttempts(val newValue: Int) : Mutation, MutationField.RemainingAttemptField

/**
 * Adds `delta` to the current visibility timeout of the messages
 *
 * SQL: `scheduled_at = scheduled_at + delta`
 */
data class AddScheduledAt(val delta: Duration) : Mutation, MutationField.ScheduledAtField

/**
 * Sets visibility timeout of the messages to `clock_timestamp() + newValue`
 *
 * SQL: `scheduled_at = clock_timestamp() + newValue`
 */
data class SetScheduledAt(val newValue: Duration) : Mutation, MutationField.ScheduledAtField



/**
 * Special internal interface to group mutations by mutated field.
 * It helps ensure that we don't mutate the same field multiple times in a single query.
 */
internal sealed interface MutationField {
    interface RemainingAttemptField : MutationField
    interface ScheduledAtField : MutationField
}
