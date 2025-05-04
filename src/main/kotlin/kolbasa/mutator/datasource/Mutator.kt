package kolbasa.mutator.datasource

import kolbasa.mutator.*
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * Base interface for all mutators
 *
 * Mutators are used to change existing messages in the queues, for example, adjust visibility timeout, remaining attempts etc.
 *
 * This is a basic interface, implementations of which may behave differently.
 *
 * Kolbasa provides a default, high-performance implementation [DatabaseMutator], which uses just plain JDBC and
 * [DataSource][javax.sql.DataSource] and doesn't require any additional dependencies. This default, provided implementation
 * completely hides database handling and transaction management from the user.
 *
 * If you require a mutator working in context of already opened transaction, see
 * [ConnectionAwareMutator][kolbasa.mutator.connection.ConnectionAwareMutator].
 */
interface Mutator {

    fun <Data, Meta : Any> addRemainingAttempts(
        queue: Queue<Data, Meta>,
        message: Id,
        delta: Int
    ): MutateResult {
        return mutate(queue, listOf(message), listOf(AddRemainingAttempts(delta)))
    }

    fun <Data, Meta : Any> setRemainingAttempts(
        queue: Queue<Data, Meta>,
        message: Id,
        newValue: Int
    ): MutateResult {
        return mutate(queue, listOf(message), listOf(SetRemainingAttempts(newValue)))
    }

    fun <Data, Meta : Any> addScheduledAt(
        queue: Queue<Data, Meta>,
        message: Id,
        delta: Duration
    ): MutateResult {
        return mutate(queue, listOf(message), listOf(AddScheduledAt(delta)))
    }

    fun <Data, Meta : Any> setScheduledAt(
        queue: Queue<Data, Meta>,
        message: Id,
        newValue: Duration
    ): MutateResult {
        return mutate(queue, listOf(message), listOf(SetScheduledAt(newValue)))
    }

    fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        messages: List<Id>,
        mutations: List<Mutation>,
    ): MutateResult

    fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        messages: List<Id>,
        mutations: List<Mutation>,
    ): CompletableFuture<MutateResult>

}
