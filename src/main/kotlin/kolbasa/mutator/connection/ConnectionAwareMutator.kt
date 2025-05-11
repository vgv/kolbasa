package kolbasa.mutator.connection

import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.*
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.sql.Connection
import java.time.Duration

interface ConnectionAwareMutator {

    fun <Data, Meta : Any> addRemainingAttempts(
        connection: Connection,
        queue: Queue<Data, Meta>,
        delta: Int,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(AddRemainingAttempts(delta)), listOf(message))
    }

    fun <Data, Meta : Any> setRemainingAttempts(
        connection: Connection,
        queue: Queue<Data, Meta>,
        newValue: Int,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(SetRemainingAttempts(newValue)), listOf(message))
    }

    fun <Data, Meta : Any> addScheduledAt(
        connection: Connection,
        queue: Queue<Data, Meta>,
        delta: Duration,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(AddScheduledAt(delta)), listOf(message))
    }

    fun <Data, Meta : Any> setScheduledAt(
        connection: Connection,
        queue: Queue<Data, Meta>,
        newValue: Duration,
        message: Id
    ): MutateResult {
        return mutate(connection, queue, listOf(SetScheduledAt(newValue)), listOf(message))
    }

    fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult

    fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult

}
