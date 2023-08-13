package kolbasa.producer

import java.sql.Connection

interface Producer<Data, Meta : Any> {
    fun send(data: Data): Long
    fun send(data: SendMessage<Data, Meta>): Long
    fun send(data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta>
}

interface ConnectionAwareProducer<Data, Meta : Any> {
    fun send(connection: Connection, data: Data): Long
    fun send(connection: Connection, data: SendMessage<Data, Meta>): Long
    fun send(connection: Connection, data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta>
}

data class SendResult<Data, Meta : Any>(
    val failedMessages: Int,
    val messages: List<MessageResult<Data, Meta>>
) {

    fun onlySuccessful(): List<MessageResult.Success<Data, Meta>> {
        return messages.filterIsInstance<MessageResult.Success<Data, Meta>>()
    }

    fun onlyFailed(): List<MessageResult.Error<Data, Meta>> {
        return messages.filterIsInstance<MessageResult.Error<Data, Meta>>()
    }

    /**
     * Convenient function to collect all failed messages to send them again
     */
    fun gatherFailedMessages(): List<SendMessage<Data, Meta>> {
        val collector = ArrayList<SendMessage<Data, Meta>>(failedMessages)
        return onlyFailed().flatMapTo(collector) { it.messages }
    }

}

sealed class MessageResult<Data, Meta : Any> {
    data class Success<Data, Meta : Any>(val id: Long, val message: SendMessage<Data, Meta>) : MessageResult<Data, Meta>()
    data class Error<Data, Meta : Any>(val error: Throwable, val messages: List<SendMessage<Data, Meta>>) : MessageResult<Data, Meta>()
}
