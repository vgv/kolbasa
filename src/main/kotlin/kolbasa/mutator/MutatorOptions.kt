package kolbasa.mutator

import java.util.concurrent.ExecutorService

data class MutatorOptions(
    val maxMutatedRowsKeepInMemory: Int = DEFAULT_MAX_MUTATED_ROWS_KEEP_IN_MEMORY,

    /**
     * Executor used to mutate messages asynchronously in [mutateAsync()][kolbasa.mutator.datasource.Mutator.mutateAsync]  methods.
     *
     * If you need to customize the executor for a specific [Mutator][kolbasa.mutator.datasource.Mutator], you can
     * provide your own [ExecutorService]. If you don't provide a custom executor, mutator will use the global,
     * default executor defined in [Kolbasa.asyncExecutor][kolbasa.Kolbasa.asyncExecutor]
     */
    val asyncExecutor: ExecutorService? = null,
) {

    companion object {
        const val DEFAULT_MAX_MUTATED_ROWS_KEEP_IN_MEMORY = 10_000
    }

}
