package kolbasa.mutator.datasource

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
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
        delta: Int,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(AddRemainingAttempts(delta)), listOf(message))
    }

    fun <Data, Meta : Any> setRemainingAttempts(
        queue: Queue<Data, Meta>,
        newValue: Int,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(SetRemainingAttempts(newValue)), listOf(message))
    }

    fun <Data, Meta : Any> addScheduledAt(
        queue: Queue<Data, Meta>,
        delta: Duration,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(AddScheduledAt(delta)), listOf(message))
    }

    fun <Data, Meta : Any> setScheduledAt(
        queue: Queue<Data, Meta>,
        newValue: Duration,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(SetScheduledAt(newValue)), listOf(message))
    }

    fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>,
    ): MutateResult

    fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult

    fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>,
    ): CompletableFuture<MutateResult>

    fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): CompletableFuture<MutateResult>
}
