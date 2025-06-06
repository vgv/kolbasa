package kolbasa.mutator.connection

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.*
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.sql.Connection
import java.time.Duration

/**
 * Base interface for all mutators that must work in context of already opened transaction.
 *
 * Mutators are used to change existing messages in the queues, for example, adjust visibility timeout, remaining attempts etc.
 *
 * Sometimes it is useful to mutate messages in context of already opened and externally managed transaction.
 *
 * For example, if you are using Hibernate, Exposed, Spring JDBC Template or another framework (even plain JDBC), you may
 * want to mutate messages in a queue and update the database in a single transaction.
 *
 * For example, you have a pending order for which you need to regularly check the payment status in a
 * third-party system. You receive a message from the `check_order_status` queue, check the order status and, if the
 * status is the same, you want to mark the time of the last check of the order in the `sale` table and, if the number
 * of attempts is over, just add a little more. If the order is paid, then just delete the message from the queue
 * because the task is completed, everything is processed.
 *
 * If the transaction will be rolled back, the message will not be deleted from the `check_order_status` queue and
 * your `sale` table won't be updated, allowing you to retry the entire operation later.
 * Otherwise, both operations will be committed.
 *
 * You don't need to handle this manually, this connection aware mutator will do it for you. All you need - provide
 * current active JDBC connection to every `mutate` method.
 *
 * In case of Hibernate, it may look like this
 * ```
 * session.doWork { connection ->
 *    consumer.receive(queue)?.let { message ->
 *       val orderIsPaid = << request to 3rd-party system >>
 *       if (!orderIsPaid) {
 *          // Update the business entity inside the same transaction
 *          sale.status = 'UNPAID'
 *          sale.lastChecked = LocalDateTime.now()
 *          session.update(sale)
 *
 *          // add remaining attempts to prolongate message life inside the same transaction
 *          if (message.remainingAttempts == 0) {
 *             mutator.addRemainingAttempts(connection, queue, 10, message.id)
 *          }
 *       } else {
 *          // Update the business entity inside the same transaction
 *          sale.status = 'PAID'
 *          session.update(sale)
 *
 *          // order is paid, let's delete the processed message inside the same transaction
 *          consumer.delete(connection, queue, message)
 *       }
 *    }
 * }
 * ```
 *
 * When Hibernate commits transaction (explicitly or, for example, when you use `@Transactional` annotation), your business
 * entity changes and message mutation will be commited at the same time.
 *
 * The same ideas work for producers or consumers too – you can build completely transactional pipeline just by connecting
 * a few producers/consumers/mutators into one "chain" using the same connection and work inside one active transaction.
 *
 * Kolbasa provides a default, high-performance implementation of [ConnectionAwareMutator]
 * (see [ConnectionAwareDatabaseMutator]), which uses just plain JDBC and doesn't require any additional dependencies.
 */
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
