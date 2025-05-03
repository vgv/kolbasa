package kolbasa.mutator

import java.time.Duration

/**
 * Marker sealed interface for all possible mutations
 */
sealed interface Mutation

data class AddRemainingAttempts(val delta: Int) : Mutation, MutationField.RemainingAttemptField
data class SetRemainingAttempts(val newValue: Int) : Mutation, MutationField.RemainingAttemptField

data class AddScheduledAt(val delta: Duration) : Mutation, MutationField.ScheduledAtField
data class SetScheduledAt(val newValue: Duration) : Mutation, MutationField.ScheduledAtField


/**
 * Special internal interface to group mutations by mutated field.
 * It helps ensure that we don't mutate the same field multiple times in a single query.
 */
internal sealed interface MutationField {
    interface RemainingAttemptField : MutationField
    interface ScheduledAtField : MutationField
}
