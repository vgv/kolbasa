package kolbasa.mutator

import kolbasa.producer.Id

data class MutateResult(
    /**
     * Number of successfully mutated messages
     */
    val mutatedMessages: Int,

    /**
     * Result of mutating each message
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
         * New visibility timeout TODO
         */
        val scheduledAt: Long,
        /**
         * New remaining attempts
         */
        val remainingAttempts: Int
    ) : MessageResult()

    data class NotFound(
        val id: Id
    ) : MessageResult()

}
