package kolbasa.mutator

import java.util.concurrent.ExecutorService

data class MutatorOptions(
    /**
     * How many mutated messages to keep in [MutateResult.messages] list when using a filter condition
     *
     * If you mutate queue by providing identifiers to mutate, you always know how many
     * results you will have - the same number as identifiers list. However, if you mutate
     * queue messages by providing filter expression, the potential response can be any size,
     * because it's hard to predict how many queue messages will be affected.
     *
     * If a queue contains billions of messages, and all of them are affected by a filter condition, it's
     * impossible to return all of those billions of messages as a result.
     *
     * This field controls how many mutated messages will be returned as a result. If there are more mutated
     * records than [maxMutatedMessagesKeepInMemory], [MutateResult.messages] will contain first [maxMutatedMessagesKeepInMemory]
     * from the result and field [MutateResult.truncated] will be true.
     *
     * Default value is [kolbasa.mutator.MutatorOptions.DEFAULT_MAX_MUTATED_MESSAGES_KEEP_IN_MEMORY]
     */
    val maxMutatedMessagesKeepInMemory: Int = DEFAULT_MAX_MUTATED_MESSAGES_KEEP_IN_MEMORY,

    /**
     * Executor used to mutate messages asynchronously in [mutateAsync()][kolbasa.mutator.datasource.Mutator.mutateAsync]  methods.
     *
     * If you need to customize the executor for a specific [Mutator][kolbasa.mutator.datasource.Mutator], you can
     * provide your own [ExecutorService]. If you don't provide a custom executor, mutator will use the global,
     * default executor defined in [Kolbasa.asyncExecutor][kolbasa.Kolbasa.asyncExecutor]
     */
    val asyncExecutor: ExecutorService? = null,
) {

    class Builder {
        private var maxMutatedMessagesKeepInMemory: Int = DEFAULT_MAX_MUTATED_MESSAGES_KEEP_IN_MEMORY
        private var asyncExecutor: ExecutorService? = null

        fun maxMutatedMessagesKeepInMemory(value: Int) = apply {
            this.maxMutatedMessagesKeepInMemory = value
        }

        fun asyncExecutor(value: ExecutorService?) = apply {
            this.asyncExecutor = value
        }

        fun build(): MutatorOptions {
            return MutatorOptions(
                maxMutatedMessagesKeepInMemory = maxMutatedMessagesKeepInMemory,
                asyncExecutor = asyncExecutor
            )
        }
    }

    internal companion object {
        const val DEFAULT_MAX_MUTATED_MESSAGES_KEEP_IN_MEMORY = 100

        val DEFAULT = MutatorOptions()

        @JvmStatic
        fun builder(): Builder = Builder()
    }

}
