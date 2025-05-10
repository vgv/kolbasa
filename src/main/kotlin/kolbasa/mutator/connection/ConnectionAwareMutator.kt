package kolbasa.mutator.connection

import kolbasa.mutator.*
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.sql.Connection
import java.time.Duration

interface ConnectionAwareMutator {

    fun <Data, Meta : Any> addRemainingAttempts(
        connection: Connection,
        queue: Queue<Data, Meta>,
        message: Id,
        delta: Int
    ): MutateResult {
        return mutate(connection, queue, listOf(message), listOf(AddRemainingAttempts(delta)))
    }

    fun <Data, Meta : Any> setRemainingAttempts(
        connection: Connection,
        queue: Queue<Data, Meta>,
        message: Id,
        newValue: Int
    ): MutateResult {
        return mutate(connection, queue, listOf(message), listOf(SetRemainingAttempts(newValue)))
    }

    fun <Data, Meta : Any> addScheduledAt(
        connection: Connection,
        queue: Queue<Data, Meta>,
        message: Id,
        delta: Duration
    ): MutateResult {
        return mutate(connection, queue, listOf(message), listOf(AddScheduledAt(delta)))
    }

    fun <Data, Meta : Any> setScheduledAt(
        connection: Connection,
        queue: Queue<Data, Meta>,
        message: Id,
        newValue: Duration
    ): MutateResult {
        return mutate(connection, queue, listOf(message), listOf(SetScheduledAt(newValue)))
    }

    fun <Data, Meta : Any> mutate(
        connection: Connection,
        queue: Queue<Data, Meta>,
        messages: List<Id>,
        mutations: List<Mutation>,
    ): MutateResult

}
