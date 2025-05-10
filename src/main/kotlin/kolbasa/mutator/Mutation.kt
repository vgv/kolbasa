package kolbasa.mutator

import java.time.Duration

sealed interface Mutation


data class AddRemainingAttempts(val delta: Int) : Mutation
data class SetRemainingAttempts(val newValue: Int) : Mutation

data class AddScheduledAt(val delta: Duration) : Mutation
data class SetScheduledAt(val newValue: Duration) : Mutation
