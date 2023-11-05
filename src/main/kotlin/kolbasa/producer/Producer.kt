package kolbasa.producer

/**
 * Base interface for all producers
 *
 * Producers are used to send messages to the queues.
 *
 * This is a basic interface, implementations of which may behave differently. Kolbasa provides a default, high-performance
 * implementation of [Producer], which uses just plain JDBC and [DataSource][javax.sql.DataSource] and doesn't require
 * any additional dependencies.
 * This default, provided implementation completely hides database handling and transaction management from the user.
 *
 * If you require a producer working in context of already opened transaction, please, look at [ConnectionAwareProducer].
 *
 * @param Data type of the message
 * @param Meta type of the metadata
 */
interface Producer<Data, Meta : Any> {

    /**
     * Just to send one message without metadata and another options
     *
     * @param data message to send
     * @return unique id of the message or throws an exception if something went wrong
     */
    fun send(data: Data): Long

    /**
     * Send one message with metadata (if any) and/or [SendOptions]
     *
     * @param data message, metadata (if any) and options (if any) to send
     * @return unique id of the message or throws an exception if something went wrong
     */
    fun send(data: SendMessage<Data, Meta>): Long

    /**
     * Send many messages with metadata (if any) and/or [SendOptions]
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param data list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun send(data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta>
}
