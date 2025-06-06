package kolbasa.mutator.connection

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.*
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.sql.Connection
import java.time.Duration

interface ConnectionAwareMutator {

    /**
     * Mutates one message in the queue
     *
     * Adds (or subtracts, if negative) `delta` to the current remaining attempts of the `message`
     *
     * @param connection JDBC connection used to mutate the message
     * @param queue queue from which to mutate a message
     * @param delta the value to add/subtract from the current remaining attempts of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> addRemainingAttempts(
        connection: Connection,
        queue: Queue<Data, Meta>,
        delta: Int,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(AddRemainingAttempts(delta)), listOf(message))
    }

    /**
     * Mutates one message in the queue
     *
     * Sets `newValue` as the new remaining attempts of the `message`
     *
     * @param connection JDBC connection used to mutate the message
     * @param queue queue from which to mutate a message
     * @param newValue the value to set as the current remaining attempts of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> setRemainingAttempts(
        connection: Connection,
        queue: Queue<Data, Meta>,
        newValue: Int,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(SetRemainingAttempts(newValue)), listOf(message))
    }

    /**
     * Mutates one message in the queue
     *
     * Adds (or subtracts, if negative) `delta` to the current visibility timeout  of the `message`
     *
     * @param connection JDBC connection used to mutate the message
     * @param queue queue from which to mutate a message
     * @param delta the value to add/subtract from the current visibility timeout of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> addScheduledAt(
        connection: Connection,
        queue: Queue<Data, Meta>,
        delta: Duration,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(AddScheduledAt(delta)), listOf(message))
    }

    /**
     * Mutates one message in the queue
     *
     * Sets `newValue` as the new visibility timeout of the `message` from the current time, e.g. `now() + newValue`
     *
     * @param connection JDBC connection used to mutate the message
     * @param queue queue from which to mutate a message
     * @param newValue the value to set as the current remaining attempts of the message
     * @param message message identifier
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> setScheduledAt(
        connection: Connection,
        queue: Queue<Data, Meta>,
        newValue: Duration,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(SetScheduledAt(newValue)), listOf(message))
    }

    /**
     * Mutates messages list in the queue
     *
     * Mutates all `messages` in one `queue` by applying `mutations` to every message
     *
     * @param connection JDBC connection used to mutate the message
     * @param queue queue from which to mutate a message
     * @param mutations mutations to apply to the `messages`
     * @param messages messages identifiers
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult

    /**
     * Mutates messages in the queue that match the custom filter condition
     *
     * Mutates all messages in one `queue` by applying `mutations` to every message that match the custom `filter` condition
     *
     * Filters can be specified using nice Kotlin lambda syntax as follows:
     * ```
     * // Try to mutate all messages with (userId<=10 OR userId=78) in the queue
     * val mutateResult = mutator.mutate(queue, listOf(AddRemainingAttempts(123))) {
     *     // Type-safe DSL to filter messages
     *     (Metadata::userId lessEq 10) or (Metadata::userId eq 78)
     * }
     * ```
     *
     * If you use Java, please take a look at [JavaField][kolbasa.consumer.JavaField] class for examples.
     *
     * @param connection JDBC connection used to mutate the message
     * @param queue queue from which to mutate a message
     * @param mutations mutations to apply to the `messages`
     * @param filter custom, user-defined filter to mutate only specific messages in the queue
     * @return [MutateResult] with the information about mutate result (success/failure)
     */
    fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult

}
