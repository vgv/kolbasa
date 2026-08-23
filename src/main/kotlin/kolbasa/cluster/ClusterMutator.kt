package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.*
import kolbasa.mutator.connection.ConnectionAwareDatabaseMutator
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.util.concurrent.CompletableFuture

/**
 * A [Mutator] that changes messages on every node of a cluster.
 *
 * How many nodes one call touches depends on what you ask for:
 * - by message id – every id carries its shard, so the ids are grouped by node and each node gets one call. An id
 *   whose shard has no node to read from at that moment, which happens while the shard is being migrated, is
 *   silently skipped: it is not an error and it is not reported, so compare
 *   [MutateResult.mutatedMessages][kolbasa.mutator.MutateResult.mutatedMessages] with the number of ids you passed
 *   if that matters to you.
 * - by filter – the filter can match messages anywhere, so every node is called.
 *
 * The results of all nodes are added up: [MutateResult.mutatedMessages][kolbasa.mutator.MutateResult.mutatedMessages]
 * is the total over the cluster. The list of changed messages is capped by
 * [MutatorOptions.maxMutatedMessagesKeepInMemory][kolbasa.mutator.MutatorOptions.maxMutatedMessagesKeepInMemory]
 * for the whole call, not per node, and
 * [MutateResult.truncated][kolbasa.mutator.MutateResult.truncated] tells you when the cap was reached.
 *
 * This class is a thin wrapper around a [Cluster]: it holds no connections of its own and creates a plain
 * [DatabaseMutator][kolbasa.mutator.datasource.DatabaseMutator] per node behind the scenes. Creating one is cheap,
 * but `mutatorOptions` is fixed at that moment, so keep one instance per set of defaults you need. The [Cluster]
 * must be initialized (see [Cluster.initAndScheduleStateUpdate]) before the first call.
 *
 * This class implements the [Mutator] interface, so, it's easy to use instad of [DatabaseMutator] if you need to
 * migrate from a single-node setup to a Kolbasa cluster.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val mutator: Mutator = ClusterMutator(cluster)
 *
 * mutator.addRemainingAttempts(orders, 5, messageId)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Mutator mutator = new ClusterMutator(cluster);
 *
 * mutator.addRemainingAttempts(orders, 5, messageId);
 * ```
 *
 * @see Cluster
 * @see kolbasa.mutator.datasource.DatabaseMutator
 */
class ClusterMutator @JvmOverloads constructor(
    private val cluster: Cluster,
    private val mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT
) : Mutator {

    override fun <Data> mutate(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult {
        val latestState = cluster.getState()
        val byNodes = latestState.mapShardsToNodes(messages) { it.shard }

        var mutatedMessagesCount = 0
        val mutatedMessagesResult = mutableListOf<MessageResult>()
        byNodes.forEach { (node, ids) ->
            if (node != null) {
                val mutator = latestState.getMutator(this, node) { nodeId, dataSource ->
                    val peer = ConnectionAwareDatabaseMutator(nodeId, mutatorOptions)
                    DatabaseMutator(dataSource, peer)
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

    override fun <Data> mutate(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition
    ): MutateResult {
        val latestState = cluster.getState()

        val allMutators = latestState.getMutators(this) { nodeId, dataSource ->
            val peer = ConnectionAwareDatabaseMutator(nodeId, mutatorOptions)
            DatabaseMutator(dataSource, peer)
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

    override fun <Data> mutateAsync(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): CompletableFuture<MutateResult> {
        // TODO: make it smarter
        val executor = MutatorSchemaHelpers.calculateAsyncExecutor(
            mutatorExecutor = mutatorOptions.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ mutate(queue, mutations, messages) }, executor)
    }

    override fun <Data> mutateAsync(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition
    ): CompletableFuture<MutateResult> {
        // TODO: make it smarter
        val executor = MutatorSchemaHelpers.calculateAsyncExecutor(
            mutatorExecutor = mutatorOptions.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ mutate(queue, mutations, filter) }, executor)
    }
}
