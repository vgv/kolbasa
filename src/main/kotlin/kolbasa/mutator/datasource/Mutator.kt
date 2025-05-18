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

    /**
     * Mutates one message in the queue
     *
     * Adds (or subtracts, if negative) `delta` to the current remaining attempts of the `message`
     *
     * @param queue queue from which to mutate a message
     * @param delta the value to add/subtract from the current remaining attempts of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> addRemainingAttempts(
        queue: Queue<Data, Meta>,
        delta: Int,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(AddRemainingAttempts(delta)), listOf(message))
    }

    /**
     * Mutates one message in the queue
     *
     * Sets `newValue` as the new remaining attempts of the `message`
     *
     * @param queue queue from which to mutate a message
     * @param newValue the value to set as the current remaining attempts of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> setRemainingAttempts(
        queue: Queue<Data, Meta>,
        newValue: Int,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(SetRemainingAttempts(newValue)), listOf(message))
    }

    /**
     * Mutates one message in the queue
     *
     * Adds (or subtracts, if negative) `delta` to the current visibility timeout  of the `message`
     *
     * @param queue queue from which to mutate a message
     * @param delta the value to add/subtract from the current visibility timeout of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> addScheduledAt(
        queue: Queue<Data, Meta>,
        delta: Duration,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(AddScheduledAt(delta)), listOf(message))
    }

    /**
     * Mutates one message in the queue
     *
     * Sets `newValue` as the new visibility timeout of the `message` from the current time, e.g. `now() + newValue`
     *
     * @param queue queue from which to mutate a message
     * @param newValue the value to set as the current remaining attempts of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> setScheduledAt(
        queue: Queue<Data, Meta>,
        newValue: Duration,
        message: Id
    ): MutateResult {
        return mutate(queue, listOf(SetScheduledAt(newValue)), listOf(message))
    }

    /**
     * Mutates messages list in the queue
     *
     * Mutates all `messages` in one `queue` by applying `mutations` to every message
     *
     * @param queue queue from which to mutate a message
     * @param mutations mutations to apply to the `messages`
     * @param messages messages identifiers
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>,
    ): MutateResult

    /**
     * Mutates messages list in the queue asynchronously
     *
     * Mutates all `messages` in one `queue` by applying `mutations` to every message
     *
     * @param queue queue from which to mutate a message
     * @param mutations mutations to apply to the `messages`
     * @param messages messages identifiers
     * @return [CompletableFuture] of the [MutateResult]
     */
    fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>,
    ): CompletableFuture<MutateResult>

    /**
     * Mutates messages in the queue that match the filter condition
     *
     * Mutates all messages in one `queue` by applying `mutations` to every message that match the `filter` condition
     *
     * @param queue queue from which to mutate a message
     * @param mutations mutations to apply to the `messages`
     * @param filter custom, user-defined filter to mutate only specific messages in the queue
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult

    /**
     * Mutates messages in the queue that match the filter condition asynchronously
     *
     * Mutates all messages in one `queue` by applying `mutations` to every message that match the `filter` condition
     *
     * @param queue queue from which to mutate a message
     * @param mutations mutations to apply to the `messages`
     * @param filter custom, user-defined filter to mutate only specific messages in the queue
     * @return [CompletableFuture] of the [MutateResult]
     */
    fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): CompletableFuture<MutateResult>
}
