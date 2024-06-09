package kolbasa.producer

import kolbasa.schema.Const

/**
 * Result of sending a batch of messages
 *
 * When we send a batch of messages, some of them may be sent successfully and some of them may fail. This class is
 * used to represent the result of sending a batch of messages.
 *
 * This class contains all unique ids of sent messages and all exceptions which occurred during sending messages.
 * It's up to you to decide what to do with failed messages. You can just ignore them, hold them in a memory for
 * some short period of time, dump to log file or resend them again.
 *
 * If you decide to resend failed messages again, the typical pattern to send messages may look like this:
 * ```
 * var result = producer.send(messages)
 * while (result.failedMessages > 0) {
 *     // resend only failed messages again
 *     result = producer.send(result.gatherFailedMessages(), <optionally reduce batchSize>)
 * }
 * ```
 * Of course, in a real application you should add some kind of limit (time or attempts number) to prevent
 * infinite loop in case of some fatal problem with the queue (database is completely down, for example).
 */
data class SendResult<Data, Meta : Any>(
    /**
     * Number of failed messages or 0 if all messages were sent successfully
     */
    val failedMessages: Int,

    /**
     * Result of sending each message
     */
    val messages: List<MessageResult<Data, Meta>>
) {

    /**
     * Convenient function to collect all successful messages
     */
    fun onlySuccessful(): List<MessageResult.Success<Data, Meta>> {
        return messages.filterIsInstance<MessageResult.Success<Data, Meta>>()
    }

    /**
     * Convenient function to collect all duplicated messages
     */
    fun onlyDuplicated(): List<MessageResult.Duplicate<Data, Meta>> {
        return messages.filterIsInstance<MessageResult.Duplicate<Data, Meta>>()
    }

    /**
     * Convenient function to collect all failed messages
     */
    fun onlyFailed(): List<MessageResult.Error<Data, Meta>> {
        return messages.filterIsInstance<MessageResult.Error<Data, Meta>>()
    }

    /**
     * Convenient function to collect all failed messages to send them again
     *
     * Differs from [onlyFailed] because this method returns not failed messages itself but messages in a format
     * appropriate for sending them again using `Producer.send(data: List<SendMessage<Data, Meta>>)`
     */
    fun gatherFailedMessages(): List<SendMessage<Data, Meta>> {
        val collector = ArrayList<SendMessage<Data, Meta>>(failedMessages)
        return onlyFailed().flatMapTo(collector) { it.messages }
    }

    internal fun extractSingularId(): Long {
        check(messages.size == 1) {
            "To extract a singular id you must have a singular response. Current response size: ${messages.size}"
        }

        return when (val message = messages.first()) {
            is MessageResult.Success -> message.id
            is MessageResult.Duplicate -> Const.RESERVED_DUPLICATE_ID
            is MessageResult.Error -> throw message.exception
        }
    }

}

/**
 * Result of sending one message.
 *
 * Every message sent to a queue has a unique id (if sent successfully) or some exception linked to it (if failed).
 * To reflect this fact, this sealed class has two implementations: [Success] and [Error].
 */
sealed class MessageResult<Data, Meta : Any> {

    /**
     * Result of successful sending of one message
     */
    data class Success<Data, Meta : Any>(
        /**
         * Unique id of the message, generated by a queue
         */
        val id: Long,

        /**
         * Message itself, if you need to associate it with the original message and generated id (for debug purposes for example)
         */
        val message: SendMessage<Data, Meta>
    ) : MessageResult<Data, Meta>()

    /**
     * Result of attempting to send a message which has the same unique key as some other message in the queue (duplicate)
     */
    data class Duplicate<Data, Meta : Any>(
        /**
         * Duplicated message that wasn't sent because there was already a message with the same unique key
         */
        val message: SendMessage<Data, Meta>
    ) : MessageResult<Data, Meta>()

    /**
     * Result of unsuccessful sending of several messages.
     *
     * Unlike [Success], this class contains a list of messages which failed to send, because, due to batching,
     * we can send several messages at once and got one exception for the whole batch. It's impossible to say which
     * message failed to send in this case, so we just return a list of messages and link all of them to one exception
     */
    data class Error<Data, Meta : Any>(
        /**
         * Exception which occurred during sending messages
         */
        val exception: Throwable,

        /**
         * List of messages which failed to send
         */
        val messages: List<SendMessage<Data, Meta>>
    ) : MessageResult<Data, Meta>()
}
