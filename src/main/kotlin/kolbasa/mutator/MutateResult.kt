package kolbasa.mutator

import kolbasa.producer.Id

data class MutateResult(

    /**
     * Number of successfully mutated messages
     */
    val mutatedMessages: Int,

    /**
     * Marker that the result list are too large and only the first N results were stored in the [messages] list.
     *
     * See [kolbasa.mutator.MutatorOptions.maxMutatedRowsKeepInMemory] for details
     */
    val truncated: Boolean,

    /**
     * Result of mutating each message or only the first N messages if there are too many mutated messages
     */
    val messages: List<MessageResult>
) {

    /**
     * Convenient function to collect all successfully mutated messages
     */
    fun onlyMutated(): List<MessageResult.Mutated> {
        return messages.onlyMutated()
    }

    /**
     * Convenient function to collect all not found messages
     */
    fun onlyNotFound(): List<MessageResult.NotFound> {
        return messages.onlyNotFound()
    }

    companion object {
        fun List<MessageResult>.onlyMutated(): List<MessageResult.Mutated> {
            return filterIsInstance<MessageResult.Mutated>()
        }

        fun List<MessageResult>.onlyNotFound(): List<MessageResult.NotFound> {
            return filterIsInstance<MessageResult.NotFound>()
        }
    }
}

/**
 * Result of mutating one message.
 *
 * Every message we would like to mutate can be successfully mutated or not found (wrong message id or message has been deleted
 * by another consumer). To reflect this fact, this sealed class has two implementations: [Mutated] and [NotFound].
 */
sealed class MessageResult {

    /**
     * Result of successful mutating of one message
     */
    data class Mutated(
        /**
         * Identifier of the mutated message
         */
        val id: Id,
        /**
         * New visibility timeout
         */
        val scheduledAt: Long,
        /**
         * New remaining attempts
         */
        val remainingAttempts: Int
    ) : MessageResult()

    /**
     * Result of unsuccessful mutating of one message
     */
    data class NotFound(
        /**
         * Identifier of the message we would like to mutate but didn't find
         */
        val id: Id
    ) : MessageResult()
}
