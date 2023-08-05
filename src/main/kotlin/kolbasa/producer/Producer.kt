package kolbasa.producer

import java.sql.Connection

interface Producer<V, M : Any> {
    fun send(data: V): Long
    fun send(data: SendMessage<V, M>): Long
    fun send(data: List<SendMessage<V, M>>): SendResult<V, M>
}

interface ConnectionAwareProducer<V, M : Any> {
    fun send(connection: Connection, data: V): Long
    fun send(connection: Connection, data: SendMessage<V, M>): Long
    fun send(connection: Connection, data: List<SendMessage<V, M>>): SendResult<V, M>
}

data class SendResult<V, M : Any>(
    val failedMessages: Int,
    val messages: List<MessageResult<V, M>>
) {

    fun onlySuccessful(): List<MessageResult.Success<V, M>> {
        return messages.filterIsInstance<MessageResult.Success<V, M>>()
    }

    fun onlyFailed(): List<MessageResult.Error<V, M>> {
        return messages.filterIsInstance<MessageResult.Error<V, M>>()
    }

    /**
     * Convenient function to collect all failed messages to send them again
     */
    fun gatherFailedMessages(): List<SendMessage<V, M>> {
        val collector = ArrayList<SendMessage<V, M>>(failedMessages)
        return onlyFailed().flatMapTo(collector) { it.messages }
    }

}

sealed class MessageResult<V, M : Any> {
    data class Success<V, M : Any>(val id: Long, val message: SendMessage<V, M>) : MessageResult<V, M>()
    data class Error<V, M : Any>(val error: Throwable, val messages: List<SendMessage<V, M>>) : MessageResult<V, M>()
}
