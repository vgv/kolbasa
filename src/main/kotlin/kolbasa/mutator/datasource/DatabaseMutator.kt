package kolbasa.mutator.datasource

import kolbasa.Kolbasa
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter
import kolbasa.mutator.MutateResult
import kolbasa.mutator.Mutation
import kolbasa.mutator.MutatorOptions
import kolbasa.mutator.MutatorSchemaHelpers
import kolbasa.mutator.connection.ConnectionAwareDatabaseMutator
import kolbasa.mutator.connection.ConnectionAwareMutator
import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.producer.Id
import kolbasa.queue.Queue
import java.util.concurrent.CompletableFuture
import javax.sql.DataSource

/**
 * Default implementation of [Mutator]
 */
class DatabaseMutator(
    private val dataSource: DataSource,
    private val peer: ConnectionAwareMutator
) : Mutator {

    @JvmOverloads
    constructor(
        dataSource: DataSource,
        mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT
    ) : this(
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseMutator(mutatorOptions)
    )

    override fun <Data> mutate(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): MutateResult {
        return dataSource.useConnection { connection ->
            peer.mutate(connection, queue, mutations, messages)
        }
    }

    override fun <Data> mutate(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition
    ): MutateResult {
        return dataSource.useConnection { connection ->
            peer.mutate(connection, queue, mutations, filter)
        }
    }

    override fun <Data> mutateAsync(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        messages: List<Id>
    ): CompletableFuture<MutateResult> {
        val executor = MutatorSchemaHelpers.calculateAsyncExecutor(
            mutatorExecutor = (peer as? ConnectionAwareDatabaseMutator)?.mutatorOptions?.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ mutate(queue, mutations, messages) }, executor)
    }

    override fun <Data> mutateAsync(
        queue: Queue<Data>,
        mutations: List<Mutation>,
        filter: Filter.() -> Condition
    ): CompletableFuture<MutateResult> {
        val executor = MutatorSchemaHelpers.calculateAsyncExecutor(
            mutatorExecutor = (peer as? ConnectionAwareDatabaseMutator)?.mutatorOptions?.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ mutate(queue, mutations, filter) }, executor)
    }
}
