package kolbasa.mutator.datasource

import kolbasa.mutator.*
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.time.Duration
import java.util.concurrent.CompletableFuture

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
