package kolbasa.mutator.datasource

import kolbasa.Kolbasa
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.MutateResult
import kolbasa.mutator.Mutation
import kolbasa.mutator.MutatorOptions
import kolbasa.mutator.connection.ConnectionAwareDatabaseMutator
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.producer.Id
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.queue.Queue
import java.util.concurrent.CompletableFuture
import javax.sql.DataSource

/**
 * Default implementation of [Mutator]
 */
class DatabaseMutator(
    private val dataSource: DataSource,
    private val mutatorOptions: MutatorOptions = MutatorOptions()
) : Mutator {

    private val peer = ConnectionAwareDatabaseMutator(mutatorOptions)

    override fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult {
        return dataSource.useConnection { connection ->
            peer.mutate(connection, queue, mutations, messages)
        }
    }

    override fun <Data, Meta : Any> mutate(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition<Meta>
    ): MutateResult {
        return dataSource.useConnection { connection ->
            peer.mutate(connection, queue, mutations, filter)
        }
    }

    override fun <Data, Meta : Any> mutateAsync(
        queue: Queue<Data, Meta>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): CompletableFuture<MutateResult> {
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
        val executor = ProducerSchemaHelpers.calculateAsyncExecutor(
            customExecutor = mutatorOptions.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ mutate(queue, mutations, filter) }, executor)
    }
}
