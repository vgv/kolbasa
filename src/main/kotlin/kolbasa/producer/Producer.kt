package kolbasa.producer

import java.sql.Connection

interface Producer<V, Meta : Any> {
    fun send(data: V): Long
    fun send(data: SendMessage<V, Meta>): Long
    fun send(data: List<SendMessage<V, Meta>>): SendResult<V, Meta>
}

interface ConnectionAwareProducer<V, Meta : Any> {
    fun send(connection: Connection, data: V): Long
    fun send(connection: Connection, data: SendMessage<V, Meta>): Long
    fun send(connection: Connection, data: List<SendMessage<V, Meta>>): SendResult<V, Meta>
}

data class SendResult<V, Meta : Any>(
    val failedMessages: Int,
    val messages: List<MessageResult<V, Meta>>
) {

    fun onlySuccessful(): List<MessageResult.Success<V, Meta>> {
        return messages.filterIsInstance<MessageResult.Success<V, Meta>>()
    }

    fun onlyFailed(): List<MessageResult.Error<V, Meta>> {
        return messages.filterIsInstance<MessageResult.Error<V, Meta>>()
    }

    /**
     * Convenient function to collect all failed messages to send them again
     */
    fun gatherFailedMessages(): List<SendMessage<V, Meta>> {
        val collector = ArrayList<SendMessage<V, Meta>>(failedMessages)
        return onlyFailed().flatMapTo(collector) { it.messages }
    }

}

sealed class MessageResult<V, Meta : Any> {
    data class Success<V, Meta : Any>(val id: Long, val message: SendMessage<V, Meta>) : MessageResult<V, Meta>()
    data class Error<V, Meta : Any>(val error: Throwable, val messages: List<SendMessage<V, Meta>>) : MessageResult<V, Meta>()
}
