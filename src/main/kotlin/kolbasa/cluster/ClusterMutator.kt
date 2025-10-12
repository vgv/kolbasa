package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.MessageResult
import kolbasa.mutator.MutateResult
import kolbasa.mutator.Mutation
import kolbasa.mutator.MutatorOptions
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.Id
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.queue.Queue
import java.util.concurrent.CompletableFuture

class ClusterMutator(
    private val cluster: Cluster,
    private val mutatorOptions: MutatorOptions = MutatorOptions()
) : Mutator {

    override fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult {
        val latestState = cluster.getState()
        val byNodes = latestState.mapShardsToNodes(messages) { it.shard }

        var mutatedMessagesCount = 0
        val mutatedMessagesResult = mutableListOf<MessageResult>()
        byNodes.forEach { (node, ids) ->
            if (node != null) {
                val mutator = latestState.getMutator(this, node) { _, dataSource ->
                    DatabaseMutator(dataSource, mutatorOptions)
                }

                val oneMutateResult = mutator.mutate(queue, mutations, ids)
                // accumulate total count
                mutatedMessagesCount += oneMutateResult.mutatedMessages
                mutatedMessagesResult += oneMutateResult.messages
            }
        }

        return MutateResult(
            mutatedMessages = mutatedMessagesCount,
            messages = mutatedMessagesResult,
            truncated = false
        )
    }

    override fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult {
        val latestState = cluster.getState()

        val allMutators = latestState.getMutators(this) { _, dataSource ->
            DatabaseMutator(dataSource, mutatorOptions)
        }

        var mutatedMessagesCount = 0
        val mutatedMessagesResult = mutableListOf<MessageResult>()

        for (mutator in allMutators) {
            val oneMutateResult = mutator.mutate(queue, mutations, filter)

            // accumulate total count
            mutatedMessagesCount += oneMutateResult.mutatedMessages

            // accumulate results, but only up to maxMutatedMessagesKeepInMemory
            val currentSize = mutatedMessagesResult.size
            val maxSize = mutatorOptions.maxMutatedMessagesKeepInMemory
            if (currentSize < maxSize) {
                val needToAdd = maxSize - currentSize
                mutatedMessagesResult += oneMutateResult.messages.subList(0, minOf(needToAdd, oneMutateResult.messages.size))
            }
        }

        return MutateResult(
            mutatedMessages = mutatedMessagesCount,
            messages = mutatedMessagesResult,
            truncated = mutatedMessagesCount > mutatorOptions.maxMutatedMessagesKeepInMemory
        )
    }

    override fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): CompletableFuture<MutateResult> {
        // TODO: make it smarter
        val executor = ProducerSchemaHelpers.calculateAsyncExecutor(
            customExecutor = mutatorOptions.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ mutate(queue, mutations, messages) }, executor)
    }

    override fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): CompletableFuture<MutateResult> {
        // TODO: make it smarter
        val executor = ProducerSchemaHelpers.calculateAsyncExecutor(
            customExecutor = mutatorOptions.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ mutate(queue, mutations, filter) }, executor)
    }
}
