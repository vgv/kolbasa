package kolbasa.producer.connection

import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import java.sql.Connection

/**
 * [ConnectionAwareProducer] interceptor interface.
 *
 * An interceptor allows you to intercept, modify, or even replace requests to a queue (or response) at any stage of execution:
 * 1) Before send
 * 2) Around send
 * 3) After send
 *
 * Using interceptors, you can do logging, tracing, auditing, and so on. Interceptors can be nested, lining up
 * handlers one after another.
 *
 * Common "send" pseudocode with two interceptors (first and second) looks like this:
 * ```
 * first.beforeSend()
 * first.aroundSend() {
 *   second.beforeSend()
 *   second.aroundSend() {
 *     producer.send() // real send
 *   }
 *   second.afterSend()
 * }
 * first.afterSend()
 * ```
 *
 * Request chain:
 * ```
 * ORIGINAL REQUEST ->
 *   first.beforeSend() ->
 *     first.aroundSend() ->
 *       second.beforeSend() ->
 *         second.aroundSend() ->
 *           real send
 * ```
 *
 * Response chain:
 * ```
 *           real result
 *         -> second.aroundSend()
 *       -> second.afterSend()
 *     -> first.aroundSend()
 *   -> first.afterSend()
 * -> FINAL RESULT
 * ```
 */
interface ConnectionAwareProducerInterceptor<Data, Meta : Any> {

    /**
     * The method will be called before sending to the queue
     * If required, the method can modify or completely replace the request
     *
     * @param connection active connection used by [ConnectionAwareProducer] to work with the queue. You can use this connection
     *      to make queries to database before send, however, every additional query will slow down working with the queue.
     *      Remember: You MUST NOT do transaction management on this connection. Do not call commit/rollback, because
     *      transaction management is entirely done by business code outside
     * @param request request to send to a queue, can be replaced or modified
     * @return the same, modified or completely new request
     */
    fun beforeSend(
        connection: Connection,
        request: SendRequest<Data, Meta>
    ): SendRequest<Data, Meta> {
        // default no-op implementation
        // just return the same request without modifying or replacing it
        return request
    }

    /**
     * The method will be called 'around' sending to the queue.
     * If required, the method can modify request, response or even prevent calling the real send method
     *
     * Since this method is 'around' interceptor (in AOP terms), it could call or not the real 'send' method. If you
     * want to invoke real send method to work with real queue, you have to explicitly invoke `call(conn, req)` in your
     * [aroundSend] implementation. It's totally fine not to call `call(conn, req)` lambda at all and create completely
     * new response on-the-fly.
     *
     * @param connection active connection used by [ConnectionAwareProducer] to work with the queue. You can use this connection
     *      to make queries to database before send, however, every additional query will slow down working with the queue.
     *      Remember: You MUST NOT do transaction management on this connection. Do not call commit/rollback, because
     *      transaction management is entirely done by business code outside
     * @param request request to send to a queue, can be replaced or modified
     * @param call lambda to call a real producer.send() method
     * @return the same (returned by `call(conn, req)` lambda), modified or completely new response
     */
    fun aroundSend(
        connection: Connection,
        request: SendRequest<Data, Meta>,
        call: (Connection, SendRequest<Data, Meta>) -> SendResult<Data, Meta>
    ): SendResult<Data, Meta> {
        // default no-op implementation
        // just invoke "call" lambda to pass control to the underlying infrastructure
        return call(connection, request)
    }

    /**
     * The method will be called after sending to the queue
     * If required, the method can modify or completely replace the response
     *
     * @param connection active connection used by [ConnectionAwareProducer] to work with the queue. You can use this connection
     *      to make queries to database before send, however, every additional query will slow down working with the queue.
     *      Remember: You MUST NOT do transaction management on this connection. Do not call commit/rollback, because
     *      transaction management is entirely done by business code outside
     * @param result result received from [aroundSend], which can be replaced, modified or left as is before being
     *      returned to the caller
     * @return the same, modified or completely new response
     */
    fun afterSend(
        connection: Connection,
        result: SendResult<Data, Meta>
    ): SendResult<Data, Meta> {
        // default no-op implementation
        // just return the same result without modifying or replacing it
        return result
    }

    companion object {

        internal tailrec fun <Data, Meta : Any> recursiveApplyInterceptors(
            interceptors: List<ConnectionAwareProducerInterceptor<Data, Meta>>,
            connection: Connection,
            request: SendRequest<Data, Meta>,
            call: (Connection, SendRequest<Data, Meta>) -> SendResult<Data, Meta>
        ): SendResult<Data, Meta> {
            if (interceptors.isEmpty()) {
                return call(connection, request)
            } else {
                val topInterceptors = interceptors.subList(0, interceptors.size - 1)
                val deepestInterceptor = interceptors.last()
                return recursiveApplyInterceptors(topInterceptors, connection, request) { conn, req ->
                    val interceptedRequest = deepestInterceptor.beforeSend(conn, req)
                    val result = deepestInterceptor.aroundSend(conn, interceptedRequest, call)
                    deepestInterceptor.afterSend(conn, result)
                }
            }
        }
    }

}
