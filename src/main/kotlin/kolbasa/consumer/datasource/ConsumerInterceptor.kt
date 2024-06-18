package kolbasa.consumer.datasource

import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.Id

interface ConsumerInterceptor<Data, Meta : Any> {

    fun beforeReceive(
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>
    ): ReceiveOptions<Meta> {
        return receiveOptions
    }

    fun aroundReceive(
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>,
        call: (Int, ReceiveOptions<Meta>) -> List<Message<Data, Meta>>
    ): List<Message<Data, Meta>> {
        return call(limit, receiveOptions)
    }

    fun afterReceive(
        result: List<Message<Data, Meta>>
    ): List<Message<Data, Meta>> {
        return result
    }

    fun beforeDelete(
        messageIds: List<Id>
    ): List<Id> {
        return messageIds
    }

    fun aroundDelete(
        messageIds: List<Id>,
        call: (List<Id>) -> Int
    ): Int {
        return call(messageIds)
    }

    fun afterDelete(
        result: Int
    ): Int {
        return result
    }

    companion object {

        internal tailrec fun <Data, Meta : Any> recursiveApplyReceiveInterceptors(
            interceptors: List<ConsumerInterceptor<Data, Meta>>,
            limit: Int,
            receiveOptions: ReceiveOptions<Meta>,
            call: (Int, ReceiveOptions<Meta>) -> List<Message<Data, Meta>>
        ): List<Message<Data, Meta>> {
            if (interceptors.isEmpty()) {
                return call(limit, receiveOptions)
            } else {
                val topInterceptors = interceptors.subList(0, interceptors.size - 1)
                val deepestInterceptor = interceptors.last()
                return recursiveApplyReceiveInterceptors(topInterceptors, limit, receiveOptions) { lim, rcvOptions ->
                    val interceptedReceiveOptions = deepestInterceptor.beforeReceive(lim, rcvOptions)
                    val result = deepestInterceptor.aroundReceive(lim, interceptedReceiveOptions, call)
                    deepestInterceptor.afterReceive(result)
                }
            }
        }

        internal tailrec fun <Data, Meta : Any> recursiveApplyDeleteInterceptors(
            interceptors: List<ConsumerInterceptor<Data, Meta>>,
            messageIds: List<Id>,
            call: (List<Id>) -> Int
        ): Int {
            if (interceptors.isEmpty()) {
                return call(messageIds)
            } else {
                val topInterceptors = interceptors.subList(0, interceptors.size - 1)
                val deepestInterceptor = interceptors.last()
                return recursiveApplyDeleteInterceptors(topInterceptors, messageIds) { msgIds ->
                    val interceptedMessageIds = deepestInterceptor.beforeDelete(msgIds)
                    val result = deepestInterceptor.aroundDelete(interceptedMessageIds, call)
                    deepestInterceptor.afterDelete(result)
                }
            }
        }
    }

}
