package kolbasa.consumer.connection

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.Id
import java.sql.Connection

interface ConnectionAwareConsumerInterceptor<Data, Meta : Any> {

    fun beforeReceive(
        connection: Connection,
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>
    ): ReceiveOptions<Meta> {
        return receiveOptions
    }

    fun aroundReceive(
        connection: Connection,
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>,
        call: (Connection, Int, ReceiveOptions<Meta>) -> List<Message<Data, Meta>>
    ): List<Message<Data, Meta>> {
        return call(connection, limit, receiveOptions)
    }

    fun afterReceive(
        connection: Connection,
        result: List<Message<Data, Meta>>
    ): List<Message<Data, Meta>> {
        return result
    }

    fun beforeDelete(
        connection: Connection,
        messageIds: List<Id>
    ): List<Id> {
        return messageIds
    }

    fun aroundDelete(
        connection: Connection,
        messageIds: List<Id>,
        call: (Connection, List<Id>) -> Int
    ): Int {
        return call(connection, messageIds)
    }

    fun afterDelete(
        connection: Connection,
        result: Int
    ): Int {
        return result
    }


    companion object {

        internal tailrec fun <Data, Meta : Any> recursiveApplyReceiveInterceptors(
            interceptors: List<ConnectionAwareConsumerInterceptor<Data, Meta>>,
            connection: Connection,
            limit: Int,
            receiveOptions: ReceiveOptions<Meta>,
            call: (Connection, Int, ReceiveOptions<Meta>) -> List<Message<Data, Meta>>
        ): List<Message<Data, Meta>> {
            if (interceptors.isEmpty()) {
                return call(connection, limit, receiveOptions)
            } else {
                val topInterceptors = interceptors.subList(0, interceptors.size - 1)
                val deepestInterceptor = interceptors.last()
                return recursiveApplyReceiveInterceptors(
                    topInterceptors,
                    connection,
                    limit,
                    receiveOptions
                ) { conn, lim, rcvOptions ->
                    val interceptedReceiveOptions = deepestInterceptor.beforeReceive(conn, lim, rcvOptions)
                    val result = deepestInterceptor.aroundReceive(conn, lim, interceptedReceiveOptions, call)
                    deepestInterceptor.afterReceive(conn, result)
                }
            }
        }

        internal tailrec fun <Data, Meta : Any> recursiveApplyDeleteInterceptors(
            interceptors: List<ConnectionAwareConsumerInterceptor<Data, Meta>>,
            connection: Connection,
            messageIds: List<Id>,
            call: (Connection, List<Id>) -> Int
        ): Int {
            if (interceptors.isEmpty()) {
                return call(connection, messageIds)
            } else {
                val topInterceptors = interceptors.subList(0, interceptors.size - 1)
                val deepestInterceptor = interceptors.last()
                return recursiveApplyDeleteInterceptors(topInterceptors, connection, messageIds) { conn, msgIds ->
                    val interceptedMessageIds = deepestInterceptor.beforeDelete(conn, msgIds)
                    val result = deepestInterceptor.aroundDelete(conn, interceptedMessageIds, call)
                    deepestInterceptor.afterDelete(conn, result)
                }
            }
        }
    }

}
